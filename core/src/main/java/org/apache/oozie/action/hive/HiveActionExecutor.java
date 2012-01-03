package org.apache.oozie.action.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.wf.ActionKillXCommand;
import org.apache.oozie.executor.jpa.HiveStatusDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.*;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XLog;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;

import javax.servlet.jsp.el.ELException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.oozie.action.ActionExecutorException.ErrorType.NON_TRANSIENT;

public class HiveActionExecutor extends ActionExecutor {

    public static final String ACTION_TYPE = "hive";

    private final XLog LOG = XLog.getLog(HiveActionExecutor.class);

    public HiveActionExecutor() {
        super(ACTION_TYPE);
    }

    @Override
    public void initActionType() {
        super.initActionType();
        registerError(JDOMException.class.getName(), NON_TRANSIENT, XML_ERROR);
        registerError(ELException.class.getName(), NON_TRANSIENT, EL_ERROR);
        //FIXME : mortbay.jetty.jsp
        //registerError(javax.el.ELException.class.getName(), NON_TRANSIENT, EL_ERROR);
    }

    @Override
    public boolean suspendJobForFail(WorkflowAction.Status status) {
        return false;
    }

    @Override
    public ELEvaluator preActionEvaluator(Context context, WorkflowAction action) {
        prepare(context, action);
        return context.getELEvaluator();
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.trace("start() begins");

        try {
            context.setStartData("-", "-", "-");

            Element actionXml = context.getActionXML();

            HiveAccessService service = Services.get().get(HiveAccessService.class);

            Attribute addressAttr = actionXml.getAttribute("address");
            Attribute timeoutAttr = actionXml.getAttribute("compile-timeout");

            ThriftHive.Client client = initialize(context, service.clientFor(addressAttr.getValue()));

            List<String> queries = getQueries(actionXml, context);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Executing queries " + queries);
            }
            String wfID = context.getWorkflow().getId();
            String actionName = action.getName();

            int timeout = timeoutAttr == null ? -1 : timeoutAttr.getIntValue();
            HiveSession session = new HiveSession(wfID, actionName, client, queries, timeout);
            service.register(action.getId(), session);

            session.execute(context, action);

        } catch (Throwable ex) {
            throw convertException(ex);
        } finally {
            LOG.trace("start() ends");
        }
    }

    private ThriftHive.Client initialize(Context context, ThriftHive.Client client) throws Exception {
        for (String prefix : new String[] {"jar", "file", "archive"}) {
            String command = command(context, prefix);
            if (command != null) {
                LOG.debug("executing : " + command);
                client.execute(command);
            }
        }
        return client;
    }

    private String command(Context context, String name) throws JDOMException {
        Attribute attr = context.getActionXML().getAttribute(name);
        if (attr != null && !attr.getValue().isEmpty()) {
            return "add " + name + " " + toAbsoluteList(context, attr.getValue());
        }
        return null;
    }

    private String toAbsoluteList(Context context, String resources) throws HadoopAccessorException {
        StringBuilder builder = new StringBuilder();
        for (String resource : resources.split(",")) {
            Path absolute = toAbsolute(context, resource.trim());
            builder.append(absolute.toString()).append(' ');
        }
        return builder.toString();
    }

    private Path toAbsolute(Context context, String path) throws HadoopAccessorException {
        Path absolute = new Path(path);
        if (!absolute.isAbsolute()) {
            absolute = new Path(context.getWorkflow().getAppPath(), absolute);
            if (!absolute.isAbsolute()) {
                absolute = absolute.makeQualified(getFileSystemFor(absolute, context));
            }
        }
        return absolute;
    }

    private void prepare(Context context, WorkflowAction action) {
        if (context.isRetry()) {
            JPAService jpa = Services.get().get(JPAService.class);
            try {
                jpa.execute(new HiveStatusDeleteJPAExecutor(action.getId()));
            } catch (JPAExecutorException e) {
                LOG.warn("Failed to remove previous query status", e);
            }
        }
    }

    private List<String> getQueries(Element actionXml, Context context) throws ActionExecutorException {
        Element script = actionXml.getChild("script", actionXml.getNamespace());
        if (script != null) {
            return loadScript(script.getTextTrim(), context);
        }
        return extractQueries(actionXml, context);
    }

    private List<String> extractQueries(Element actionXml, Context context) throws ActionExecutorException {
        ELEvaluator evaluator = context.getELEvaluator();
        List<String> queries = new ArrayList<String>();
        for (Object element : actionXml.getChildren("query", actionXml.getNamespace())) {
            String sql = ((Element)element).getTextTrim();
            queries.add(evaluate(evaluator, sql));
        }
        return queries;
    }

    private List<String> loadScript(String script, Context context) throws ActionExecutorException {

        ELEvaluator evaluator = context.getELEvaluator();

        try {
            Path path = toAbsolute(context, script);
            FileSystem fs = getFileSystemFor(path, context);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));

            List<String> result = new ArrayList<String>();

            String line;
            StringBuilder builder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                String trimed = line.trim();
                if (trimed.isEmpty()) {
                    continue;
                }
                if (trimed.endsWith(";")) {
                    builder.append(line.substring(0, line.lastIndexOf(';')));
                    result.add(evaluate(evaluator, builder.toString()));
                    builder.setLength(0);
                } else {
                    builder.append(line);
                }
            }
            if (builder.length() != 0) {
                throw new ActionExecutorException(NON_TRANSIENT, "HIVE-001", "Invalid end of script");
            }
            return result;
        } catch (ActionExecutorException e) {
            throw e;
        } catch (Throwable e) {
            throw new ActionExecutorException(NON_TRANSIENT, "HIVE-002", "Failed to load script file {0}", script, e);
        }
    }

    private String evaluate(ELEvaluator evaluator, String sql) throws ActionExecutorException {
        try {
            return evaluator.evaluate(sql, String.class);
        } catch (Throwable e) {
            throw new ActionExecutorException(NON_TRANSIENT, "HIVE-000", "Failed to evaluate sql {0}", sql, e);
        }
    }

    private FileSystem getFileSystemFor(Path path, Context context) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        return Services.get().get(HadoopAccessorService.class).createFileSystem(user, path.toUri(), new Configuration());
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.debug("end called " + action.getId());
        if (action.getExternalStatus().equals("OK")) {
            context.setEndData(WorkflowAction.Status.OK, "OK");
        } else if (action.getExternalStatus().equals("ERROR")) {
            context.setEndData(WorkflowAction.Status.ERROR, "ERROR");
        } else {
            throw new IllegalStateException("Invalid external status [" + action.getExternalStatus() + "] for Fs Node");
        }
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.debug("check called for " + action.getId());
        HiveAccessService service = Services.get().get(HiveAccessService.class);
        HiveSession session = service.getRunningSession(action.getId());
        session.check(context, action);
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.info("kill called for " + action.getId());
        HiveAccessService service = Services.get().get(HiveAccessService.class);
        HiveSession session = service.peekRunningStatus(action.getId());
        if (session == null) {
            LOG.info("failed to find hive status for " + action.getId());
            return;
        }
        try {
            if (session.kill(context)) {
                CallableQueueService callables = Services.get().get(CallableQueueService.class);
                callables.queue(new ActionKillXCommand(action.getId(), action.getType()), 30000);
            }
        } catch (Exception e) {
            LOG.info("failed to kill external jobs", e);
        }
    }

    @Override
    public boolean isCompleted(String actionID, String externalStatus, Properties actionData) {
        LOG.info("external callback for " + actionID + " = " + externalStatus + ", " + actionData);
        HiveAccessService service = Services.get().get(HiveAccessService.class);
        HiveSession session = service.peekRunningStatus(actionID);
        if (session == null) {
            LOG.info("external callback called but action " + actionID + " is not running");
        } else {
            String queryId = actionData.getProperty("queryId");
            String stageId = actionData.getProperty("stageId");
            String jobId = actionData.getProperty("jobId");
            session.callback(queryId, stageId, jobId, externalStatus);
        }
        return false;
    }
}
