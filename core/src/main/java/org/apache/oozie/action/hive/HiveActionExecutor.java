package org.apache.oozie.action.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.wf.ActionKillXCommand;
import org.apache.oozie.command.wf.ActionStartXCommand;
import org.apache.oozie.executor.jpa.HiveStatusDeleteJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.HiveAccessService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.JobsConcurrencyService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.thrift.TException;
import org.jdom.Attribute;
import org.jdom.Element;
import org.jdom.JDOMException;

import javax.servlet.jsp.el.ELException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import static org.apache.oozie.action.ActionExecutorException.ErrorType.NON_TRANSIENT;
import static org.apache.oozie.action.ActionExecutorException.ErrorType.TRANSIENT;

public class HiveActionExecutor extends ActionExecutor {

    public static final String THRIFT_ERROR = "THRIFT_ERROR";
    public static final String HIVE_SERVER_ERROR = "HIVE_SERVER_ERROR";

    public static final int DEFAULT_MAX_FETCH = 20;
    public static final boolean DEFAULT_MONITORING = true;

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
        registerError(TException.class.getName(), TRANSIENT, THRIFT_ERROR);
        registerError(HiveServerException.class.getName(), NON_TRANSIENT, HIVE_SERVER_ERROR);
    }

    @Override
    public ELEvaluator preActionEvaluator(Context context, WorkflowAction action) {
        prepare(context, action);
        return context.getELEvaluator();
    }

    @Override
    public void updateAttributes(WorkflowActionBean wfAction, Map<String, String> updates) throws Exception {
        Element config = XmlUtils.parseXml(wfAction.getConf());
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            LOG.info("Updating action configuration " + entry);
            if (name.equals("address")) {
                setAttribute(config, "address", value);
            } else if (name.equals("jar")) {
                setAttribute(config, "jar", value);
            } else if (name.equals("file")) {
                setAttribute(config, "file", value);
            } else if (name.equals("archive")) {
                setAttribute(config, "archive", value);
            } else if (name.equals("max-fetch")) {
                setAttribute(config, "max-fetch", value);
            } else if (name.equals("monitoring")) {
                setAttribute(config, "monitoring", value);
            } else if (name.equals("script")) {
                setElement(config, "script", value);
            } else if (name.equals("query")) {
                setElement(config, "query", value);
            } else if (name.equals("queries")) {
                setElement(config, "query", parseScript(null, value));
            } else {
                throw new CommandException(ErrorCode.E0828, wfAction.getType(), name);
            }
        }
        wfAction.setConf(XmlUtils.prettyPrint(config).toString());
    }

    @Override
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.trace("start() begins");

        try {
            WorkflowJob workflow = context.getWorkflow();

            String wfID = workflow.getId();
            String actionName = action.getName();

            context.setStartData("-", "-", "-");
            Element actionXml = context.getActionXML();

            HiveAccessService service = Services.get().get(HiveAccessService.class);

            Attribute addressAttr = actionXml.getAttribute("address");
            Attribute maxFetchAttr = actionXml.getAttribute("max-fetch");
            Attribute monitorAttr = actionXml.getAttribute("monitoring");

            String hiveAccessUser = new XConfiguration(new StringReader(context.getWorkflow().getConf()))
                    .get("hive.user.name", workflow.getUser());
            HiveTClient client = initialize(context, service.clientFor(addressAttr.getValue(), hiveAccessUser));

            String[] queries = getQueries(actionXml, context);
            if (LOG.isDebugEnabled()) {
                LOG.debug("On executing queries : " + Arrays.toString(queries));
            }

            int maxPatch = maxFetchAttr == null ? DEFAULT_MAX_FETCH : maxFetchAttr.getIntValue();
            boolean monitoring = monitorAttr == null ? DEFAULT_MONITORING : monitorAttr.getBooleanValue();
            HiveSession session = new HiveSession(wfID, actionName, monitoring, client, queries, maxPatch);
            service.register(action.getId(), session);

            session.initialize(context);
            session.execute(context, action);

        } catch (Throwable ex) {
            throw convertException(ex);
        } finally {
            LOG.trace("start() ends");
        }
    }

    private HiveTClient initialize(Context context, HiveTClient client) throws Exception {
        for (String prefix : new String[] {"jar", "file", "archive"}) {
            String command = command(context, prefix);
            if (command != null) {
                LOG.debug("Executing initialization command : " + command);
                client.executeTransient(command);
                client.clear();
            }
        }
        return client;
    }

    private String command(Context context, String name) throws JDOMException, HadoopAccessorException {
        Attribute attr = context.getActionXML().getAttribute(name);
        if (attr != null && !attr.getValue().isEmpty()) {
            return "add " + name + " " + toAbsoluteList(context, attr.getValue());
        }
        return null;
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

    private FileSystem getFileSystemFor(Path path, Context context) throws HadoopAccessorException {
        String user = context.getWorkflow().getUser();
        return Services.get().get(HadoopAccessorService.class).createFileSystem(user, path.toUri(), new Configuration());
    }

    @Override
    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.debug("Action end requested");
        if (action.getExternalStatus().equals("OK")) {
            context.setEndData(WorkflowAction.Status.OK, "OK");
        } else if (action.getExternalStatus().equals("ERROR")) {
            context.setEndData(WorkflowAction.Status.ERROR, "ERROR");
        } else {
            context.setEndData(WorkflowAction.Status.ERROR, "ERROR");
        }
    }

    @Override
    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.debug("Action check requested");
        HiveAccessService service = Services.get().get(HiveAccessService.class);
        HiveStatus status = service.peekRunningStatus(action.getId());
        if (status == null) {
            WorkflowActionBean actionBean = (WorkflowActionBean) action;
            boolean isServerAlive = Services.get().get(JobsConcurrencyService.class).isAlive(actionBean.getOozieId());
            LOG.info("No Running HiveSession: isAlive[{0}] {1}", actionBean.getOozieId(), isServerAlive);
            if (Services.get().getConf().getBoolean("oozie.action.hive.session.fail.retry", false)) {
                retry(actionBean);
            }
            else {
                context.setExecutionData("FAILED", null);
                throw convertException(new IllegalArgumentException("No Running HiveSession"));
            }
        }
        if (status instanceof HiveSession) {
            try {
                HiveSession session = (HiveSession) status;
                if (!session.check(context)) {
                    Configuration conf = Services.get().get(ConfigurationService.class).getConf();
                    if(conf.getBoolean(HiveSession.PING_ENABLED, false)){
                        String hiveAccessUser = new XConfiguration(new StringReader(context.getWorkflow().getConf()))
                                .get("hive.user.name", context.getWorkflow().getUser());
                        session.checkConnection(hiveAccessUser);
                    }
                }
            } catch (Exception e) {
                throw convertException(e);
            }
        }
    }

    @Override
    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.debug("Action kill requested");
        HiveAccessService service = Services.get().get(HiveAccessService.class);
        HiveStatus session = service.peekRunningStatus(action.getId());
        if (session == null) {
            WorkflowActionBean actionBean = (WorkflowActionBean) action;
            boolean isActionForThisServer = Services.get().get(JobsConcurrencyService.class).isActionForThisServer(actionBean);
            LOG.info("No Running HiveSession: oozieId [{0}] isActionForThisServer [{1}]", actionBean.getOozieId(),
                    isActionForThisServer);
            cancel(actionBean);
            return;
        }
        try {
            boolean finished = false;
            int retryCount = 0;
            while (retryCount < 3 && !(finished = service.actionFinished(action.getId()))) {
                LOG.info("Can not finish action [{0}], retry 10 secs later", action.getId());
                retryCount++;
                Thread.sleep(10000);
            }
            if (!finished) {
                LOG.info("Can not finish action [{0}] ", action.getId());
            }
        } catch (Exception e) {
            LOG.info("Failed to kill external jobs", e);
        }
    }

    @Override
    public boolean isCompleted(String actionID, String externalStatus, Properties actionData) {
        LOG.debug("Action callback with status = " + externalStatus + ", data = " + actionData);
        HiveAccessService service = Services.get().get(HiveAccessService.class);
        HiveStatus session = service.peekRunningStatus(actionID);
        if (session == null) {
            LOG.info("Action callback arrived for No Running HiveSession");
        } else {
            String queryId = actionData.getProperty("queryId");
            String stageId = actionData.getProperty("stageId");
            String jobId = actionData.getProperty("jobId");
            session.callback(queryId, stageId, jobId, externalStatus);
        }
        return false;
    }

    private void retry(WorkflowActionBean actionBean) {
        cancel(actionBean);

        actionBean.setStatus(WorkflowAction.Status.USER_RETRY);

        LOG.info("Preparing retry after 10secs: action {0}, status {1}, pending {2}", actionBean.getId(), actionBean.getStats(),
                actionBean.getPending());
        Services.get().get(CallableQueueService.class).queue(new ActionStartXCommand(actionBean.getId(), actionBean.getType())
                , 10000);
    }

    /**
     * Cancel hive action: shutdown hivestatus.
     */
    private void cancel(WorkflowActionBean actionBean) {
        HiveStatus hiveStatus = Services.get().get(HiveAccessService.class).loadRunningStatus(actionBean.getId(), true);
        hiveStatus.shutdown(true);
        LOG.info("cancel action [{0}] ", actionBean.getId());
    }
}
