package org.apache.oozie;

import jline.ConsoleReader;
import jline.SimpleCompletor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.HiveStatus;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.XmlUtils;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class SimpleClient {

    private static enum CONTEXT {
        WF{
            String pathKey() { return OozieClient.APP_PATH; }
            Map<String, Method> getJobProperties() { return WF_JOB_PROPS; }
            Map<String, Method> getActionProperties() { return WF_ACTION_PROPS; }
            Object getJobInfo(OozieClient client, String jobID) throws OozieClientException {
                return client.getJobInfo(jobID);
            }
            List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getJobsInfo(pattern, start, length);
            }
            List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getActionsInfo(pattern, start, length);
            }
            String getJobId(Object job) { return ((WorkflowJob)job).getId();}
            String getJobConf(Object job) { return ((WorkflowJob)job).getConf();}
            List getActions(Object job) { return ((WorkflowJob)job).getActions();}
            boolean isJobTerminal(Object job) {
                WorkflowJob.Status status = ((WorkflowJob)job).getStatus();
                return status == WorkflowJob.Status.KILLED || status == WorkflowJob.Status.FAILED || status == WorkflowJob.Status.SUCCEEDED;
            }
            String getActionConf(Object action) { return ((WorkflowAction)action).getConf();}
        },
        COORD{
            String pathKey() { return OozieClient.COORDINATOR_APP_PATH; }
            Map<String, Method> getJobProperties() { return COORD_JOB_PROPS; }
            Map<String, Method> getActionProperties() { return COORD_ACTION_PROPS; }
            Object getJobInfo(OozieClient client, String jobID) throws OozieClientException {
                return client.getCoordJobInfo(jobID);
            }
            List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getCoordJobsInfo(pattern, start, length);
            }
            List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                throw new UnsupportedOperationException("not-yet");
            }
            String getJobId(Object job) { return ((CoordinatorJob)job).getId();}
            String getJobConf(Object job) { return ((CoordinatorJob)job).getConf();}
            List getActions(Object job) { return ((CoordinatorJob)job).getActions();}
            boolean isJobTerminal(Object job) {
                CoordinatorJob.Status status = ((CoordinatorJob)job).getStatus();
                return status == CoordinatorJob.Status.KILLED || status == CoordinatorJob.Status.FAILED || status == CoordinatorJob.Status.SUCCEEDED;
            }
            String getActionConf(Object action) { return ((CoordinatorAction)action).getCreatedConf();}
        },
        BUNDLE{
            String pathKey() { return OozieClient.BUNDLE_APP_PATH; }
            Map<String, Method> getJobProperties() { return BUNDLE_JOB_PROPS; }
            Map<String, Method> getActionProperties() { return BUNDLE_ACTION_PROPS; }
            Object getJobInfo(OozieClient client, String jobID) throws OozieClientException {
                return client.getBundleJobInfo(jobID);
            }
            List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getBundleJobsInfo(pattern, start, length);
            }
            List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                throw new UnsupportedOperationException("not-yet");
            }
            String getJobId(Object job) { return ((BundleJob)job).getId();}
            String getJobConf(Object job) { return ((BundleJob)job).getConf();}
            List getActions(Object job) { return Collections.emptyList();}

            boolean isJobTerminal(Object job) {
                BundleJob.Status status = ((BundleJob)job).getStatus();
                return status == BundleJob.Status.KILLED || status == BundleJob.Status.FAILED || status == BundleJob.Status.SUCCEEDED;
            }
            String getActionConf(Object action) { return "";}
        };

        abstract String pathKey();

        abstract Map<String, Method> getJobProperties();

        abstract Map<String, Method> getActionProperties();

        abstract Object getJobInfo(OozieClient client, String jobID) throws OozieClientException;

        abstract List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException;

        abstract List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException;

        abstract List getActions(Object job);

        abstract String getJobId(Object job);

        abstract String getJobConf(Object job);

        abstract boolean isJobTerminal(Object job);

        abstract String getActionConf(Object action);
    }

    static Map<String, Method> parse(Class clazz) {
        Map<String, Method> props = new HashMap<String, Method>();
        for (Method method : clazz.getMethods()) {
            String name = method.getName();
            if (name.startsWith("get") && method.getParameterTypes().length == 0 &&
                    method.getReturnType() != Void.class) {
                String key = Character.toLowerCase(name.charAt(3)) + name.substring(4);
                props.put(key, method);
            }
        }
        return props;
    }

    private static final Map<String, Method> WF_JOB_PROPS = parse(WorkflowJob.class);
    private static final Map<String, Method> WF_ACTION_PROPS = parse(WorkflowAction.class);
    private static final Map<String, Method> COORD_JOB_PROPS = parse(CoordinatorJob.class);
    private static final Map<String, Method> COORD_ACTION_PROPS = parse(CoordinatorAction.class);
    private static final Map<String, Method> BUNDLE_JOB_PROPS = parse(BundleJob.class);
    private static final Map<String, Method> BUNDLE_ACTION_PROPS = parse(BundleActionBean.class);

    private static final long DEFAULT_POLL_INTERVAL = 2000;
    private static final String ACTION_ALL = "$ALL";
    private static final String CURRENT_JOB_ID = "$CUR";

    private static enum COMMAND {
        submit, start, run, rerun, kill, killall, suspend, resume, update, status, poll, cancel, log, xml, jobs, actions, use, context, quit
    }

    CONTEXT context = CONTEXT.WF;
    OozieClient client;

    List<Polling> pollings = new ArrayList<Polling>();

    String jobID = null;
    List<String> jobIDs = new ArrayList<String>();

    SimpleClient(OozieClient client) {
        this.client = client;
    }

    public void execute(String appPath) throws Exception {

        ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);

        SimpleCompletor completor = new SimpleCompletor(new String[0]);
        for (COMMAND command : COMMAND.values()) {
            completor.addCandidateString(command.name());
        }

        reader.addCompletor(completor);

        if (appPath != null) {
            executeCommand(appPath, completor);
        }

        String line;
        while ((line = reader.readLine(context + ">")) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (!executeCommand(line, completor)) {
                break;
            }
        }
    }

    private boolean executeCommand(String line, SimpleCompletor completor) {
        if (jobID != null) {
            line.replaceAll(CURRENT_JOB_ID, jobID);
        }
        try {
            String[] commands = line.split("(\\s*,\\s*)|(\\s+)");
            if (commands[0].equals("help")) {
                System.out.println(completor.getCandidates());
            } else if (commands[0].equals("submit")) {
                String newJobID = client.submit(jobDescription(commands[1]));
                System.out.println("submitted job " + client.getJobInfo(newJobID) + (jobID == null ? "" : ", replacing " + jobID));
                jobID = newJobID;
            } else if (commands[0].equals("start")) {
                client.start(jobID != null ? jobID : commands[1]);
                poll(jobID != null ? jobID : commands[1]);
            } else if (commands[0].equals("run")) {
                String newJobID = client.run(jobDescription(commands[1]));
                System.out.println("running job " + client.getJobInfo(newJobID) + (jobID == null ? "" : ", replacing " + jobID));
                jobID = newJobID;
                poll(newJobID);
            } else if (commands[0].equals("rerun")) {
                String runJobId = commands.length > 1 ? commands[1] : jobID;
                String runAppPath = client.getJobInfo(runJobId).getAppPath();
                client.reRun(runJobId, jobDescription(runAppPath));
                System.out.println("rerunning job " + client.getJobInfo(runJobId) + (jobID == null ? "" : ", replacing " + jobID));
                jobID = runJobId;
                poll(runJobId);
            } else if (commands[0].equals("kill")) {
                client.kill(jobID != null ? commands.length == 1 ? jobID : jobID + "@" + commands[1] : commands[1]);
            } else if (commands[0].equals("killall")) {
                for (Object job : context.getJobsInfo(client, commands.length > 1 ? commands[1] : "", 1, 1000)) {
                    if (!context.isJobTerminal(job)) {
                        System.out.println("killing.. " + context.getJobId(job));
                        client.kill(context.getJobId(job));
                    }
                }
            } else if (commands[0].equals("suspend")) {
                client.suspend(getID(commands));
            } else if (commands[0].equals("resume")) {
                client.resume(getID(commands));
            } else if (commands[0].equals("update")) {
                String actionID = jobID != null ? jobID + "@" + commands[1] : commands[1];
                String remain = line.substring(line.indexOf(commands[1]) + commands[1].length());
                client.update(actionID, parseParams(remain));
            } else if (commands[0].equals("status")) {
                System.out.println(getStatus(jobID != null ? jobID : commands[1]));
            } else if (commands[0].equals("poll")) {
                poll(jobID != null ? jobID : commands[1]);
            } else if (commands[0].equals("cancel")) {
                if (pollings.size() > 0) {
                    pollings.get(0).cancel = true;
                    pollings.get(0).join();
                }
            } else if (commands[0].equals("log")) {
                client.getLog(getID(commands), System.out);
            } else if (commands[0].equals("xml")) {
                String ID = getID(commands);
                int index = ID.indexOf('@');
                if (index >= 0) {
                    if (ID.substring(index + 1).equals(ACTION_ALL)) {
                      WorkflowJob job = client.getJobInfo(ID.substring(0, index));
                      for (WorkflowAction action : job.getActions()) {
                        System.out.println("---------------------- " + action.getName() + " ----------------------");
                        System.out.println(XmlUtils.prettyPrint(action.getConf()).toString());
                      }
                    } else {
                      WorkflowAction action = client.getWorkflowActionInfo(ID);
                      System.out.println("---------------------- " + action.getName() + " ----------------------");
                      System.out.println(XmlUtils.prettyPrint(action.getConf()).toString());
                    }
                } else {
                    System.out.println(XmlUtils.prettyPrint(context.getJobConf(context.getJobInfo(client, ID))));
                }
            } else if (commands[0].equals("data")) {
                String ID = getID(commands);
                int index = ID.indexOf('@');
                if (index >= 0 && !ID.substring(index + 1).equals(ACTION_ALL)) {
                    WorkflowAction action = client.getWorkflowActionInfo(ID);
                    if (action.getData() != null && !action.getData().isEmpty()) {
                        System.out.println("---------------------- " + action.getName() + " ----------------------");
                        System.out.println(action.getData());
                    }
                } else {
                    WorkflowJob job = client.getJobInfo(index < 0 ? ID : ID.substring(0, index));
                    for (WorkflowAction action : job.getActions()) {
                        if (action.getData() != null && !action.getData().isEmpty()) {
                            System.out.println("---------------------- " + action.getName() + " ----------------------");
                            System.out.println(action.getData());
                        }
                    }
                }
            } else if (commands[0].equals("jobs")) {
                FilterParams params = new FilterParams(context.getJobProperties(), commands);
                CONTEXT context = params.getContext();
                if (params.jobID != null) {
                    params.appendFilter("id=" + params.jobID);
                }
                jobIDs.clear();

                int index = 0;
                for (Object job : context.getJobsInfo(client, params.filter, params.start, params.length)) {
                    System.out.printf("[%1$2d] %2$s\n", index++, params.toString(job));
                    jobIDs.add(context.getJobId(job));
                    if (params.dumpXML) {
                        System.out.println(XmlUtils.prettyPrint(context.getJobConf(job)).toString());
                    }
                }
            } else if (commands[0].equals("actions")) {
                FilterParams params = new FilterParams(context.getActionProperties(), commands);
                CONTEXT context = params.getContext();
                if (params.jobID != null || jobID != null) {
                    params.appendFilter("wfId=" + (params.jobID != null ? params.jobID : jobID));
                }
                List actions = context.getActionsInfo(client, params.filter, params.start, params.length);
                for (Object action : actions) {
                    System.out.println(params.toString(action));
                    if (params.dumpXML) {
                        System.out.println(XmlUtils.prettyPrint(context.getActionConf(action)).toString());
                    }
                }
            } else if (commands[0].equals("use")) {
                String newJobID;
                if (commands.length > 1 && commands[1].length() > 0 && isJobID(commands[1])) {
                    CONTEXT context = getContext(commands[1]);
                    newJobID = context.getJobId(context.getJobInfo(client, commands[1]));
                    this.context = context;
                } else {
                    int index = Integer.parseInt(commands[1]);
                    if (jobIDs.size() <= index) {
                        jobIDs.clear();
                        for (Object job: context.getJobsInfo(client, "", 1, index + 1)) {
                            jobIDs.add(context.getJobId(job));
                        }
                    }
                    newJobID = jobIDs.get(index);
                }
                System.out.println("set default job : " + newJobID + (jobID == null ? "" : ", replacing " + jobID));
                jobID = newJobID;
            } else if (commands[0].equals("quit")) {
                return false;
            } else if (commands[0].equals("context")) {
                try {
                    context = CONTEXT.valueOf(commands[1].toUpperCase());
                } catch (IllegalArgumentException e) {
                    System.out.println("supports " + Arrays.toString(CONTEXT.values()) + " only");
                }
            } else {
                System.err.println("invalid command " + line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private String getID(String[] commands) {
        return commands.length >= 2 && commands[1].contains("@") ? commands[1] :
                jobID != null ? commands.length == 1 ? jobID : jobID + "@" + commands[1] : commands[1];
    }

    private static Pattern JOB_ID_PATTERN = Pattern.compile("\\d{7}-\\d{15}-\\S+-\\S+-[WCB]");
    private static Pattern ACTION_ID_PATTERN = Pattern.compile(JOB_ID_PATTERN + "@\\S+");

    private static boolean isJobID(String string) {
        return JOB_ID_PATTERN.matcher(string).matches();
    }

    private static boolean isActionID(String string) {
        return ACTION_ID_PATTERN.matcher(string).matches();
    }

    private CONTEXT getContext(String id) {
        if (id.contains("@")) {
            id = id.substring(0, id.indexOf("@"));
        }
        char type = id.charAt(id.length() - 1);
        switch (type) {
            case 'W':
                return CONTEXT.WF;
            case 'C':
                return CONTEXT.COORD;
            case 'B':
                return CONTEXT.BUNDLE;
            default:
                throw new IllegalArgumentException("invalid job id " + id);
        }
    }

    private class FilterParams {

        boolean dumpXML;
        String filter = "";
        String jobID;
        String actionID;
        int start = -1;
        int length = -1;
        Object[] props;

        FilterParams(Map<String, Method> maps, String[] command) {
            int i = 1;
            for (; i < command.length; i++) {
                if (command[i].equals("-xml")) {
                    dumpXML = true;
                } else if (isJobID(command[i])) {
                    jobID = command[i];
                } else if (isActionID(command[i])) {
                    actionID = command[i];
                } else if (isNumeric(command[i])) {
                    if (start < 0) {
                        start = Integer.valueOf(command[i]);
                    } else if (length < 0) {
                        length = Integer.valueOf(command[i]);
                    } else {
                        throw new IllegalArgumentException("what is this for ? " + command[i]);
                    }
                } else {
                    if (command[i].contains("=")) {
                        appendFilter(command[i]);
                    } else {
                        throw new IllegalArgumentException("what is this for ? " + command[i]);
                    }
                }
            }
            if (start < 0 && length < 0) {
                start = 1; length = 50;
            }
            if (start >= 0 && length < 0 || start < 0 && length >= 0) {
                throw new IllegalArgumentException();
            }
            props = accessor(maps, Arrays.copyOfRange(command, i, command.length));
        }

        public void appendFilter(String append) {
            filter = filter.isEmpty() ? append : filter + ";" + append;
        }

        private boolean isNumeric(String string) {
            for (char achar : string.toCharArray()) {
                if (!Character.isDigit(achar)) {
                    return false;
                }
            }
            return true;
        }

        private Object[] accessor(Map<String, Method> maps, String... keys) {
            List<Object> methods = new ArrayList<Object>();
            for (String key : keys) {
                if (maps.containsKey(key)) {
                    methods.add(key);
                    methods.add(maps.get(key));
                    continue;
                }
                Pattern pattern = Pattern.compile(key.replaceAll("\\*", ".*").replaceAll("\\?", "."));
                for (Map.Entry<String, Method> entry : maps.entrySet()) {
                    if (pattern.matcher(entry.getKey()).matches()) {
                        methods.add(entry.getKey());
                        methods.add(entry.getValue());
                    }
                }
            }
            return methods.toArray();
        }

        private String toString(Object instance) throws Exception {
            StringBuilder builder = new StringBuilder(instance.toString());
            for (int i = 0; i < props.length;) {
                builder.append(", ").append(props[i++]).append('=').append(((Method) props[i++]).invoke(instance));
            }
            return builder.toString();
        }

        public CONTEXT getContext() {
            if (jobID != null) {
                return SimpleClient.this.getContext(jobID);
            }
            if (actionID != null) {
                return SimpleClient.this.getContext(actionID);
            }
            return context;
        }
    }

    private String getStatus(String jobId) throws OozieClientException {
        StringBuilder builder = new StringBuilder();
        switch (getContext(jobId)) {
            case WF:
                WorkflowJob workflow = client.getJobInfo(jobId);
                builder.append(workflow.getId()).append(":").append(workflow.getStatus()).append('\n');
                for (WorkflowAction action : workflow.getActions()) {
                    builder.append(action.toString()).append('\n');
                    if (action.getType().equals("hive")) {
                        for (HiveStatus status : client.getHiveStatusListForActionID(action.getId())) {
                            builder.append("   ").append(status.getStageId()).append("-->");
                            builder.append(status.getJobId()).append(":").append(status.getStatus()).append('\n');
                        }
                    }
                }
                break;
            case COORD:
                CoordinatorJob coord = client.getCoordJobInfo(jobId);
                builder.append(coord.getId()).append(":").append(coord.getStatus()).append('\n');
                for (CoordinatorAction action : coord.getActions()) {
                    builder.append(action.toString()).append('\n');
                }
                break;
            case BUNDLE:
                BundleJob bundle = client.getBundleJobInfo(jobId);
                builder.append(bundle.getId()).append(":").append(bundle.getStatus()).append('\n');
                for (CoordinatorJob action : bundle.getCoordinators()) {
                    builder.append(action.toString()).append('\n');
                }
                break;
        }
        return builder.toString();
    }

    private void poll(String jobID) {
        poll(jobID, DEFAULT_POLL_INTERVAL);
    }

    private void poll(String jobID, long interval) {
        Polling polling = new Polling(jobID, interval);
        pollings.add(polling);
        polling.start();
    }

    private class Polling extends Thread {

        String jobId;
        long interval;
        String previous;

        volatile boolean cancel;

        Polling(String jobId, long interval) {
            this.jobId = jobId;
            this.interval = interval;
        }

        public void run() {
            try {
                System.out.println("\n--------------------------------------------");
                while (!cancel) {
                    String result = getStatus(jobId);
                    if (!result.equals(previous)) {
                        System.out.println(result);
                        previous = result;
                        String first = result.split("\\s")[0];
                        if (first.endsWith(":SUCCEEDED") || first.endsWith(":FAILED")
                                || first.endsWith(":KILLED") || first.endsWith(":SUSPENDED")) {
                            break;
                        }
                    }
                    Thread.sleep(interval);
                }
                System.out.println("--------------------------------------------\n");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                pollings.remove(this);
            }
        }
    }

    private Map<String, String> parseParams(String line) {
        line = line.trim();
        Map<String, String> params = new HashMap<String, String>();
        for (String param : line.split(",[\\s]*")) {
            int eq = param.indexOf('=');
            String key = eq < 0 ? param : param.substring(0, eq).trim();
            String value = eq < 0 ? null : param.substring(eq + 1).trim();
            params.put(key, value == null ? value : value.replaceAll(";", ";\n"));
        }
        return params;
    }

    private Properties jobDescription(String appPath) throws IOException {
        Properties props = new Properties();
        Path path = new Path(appPath, "job.properties");
        FileSystem fs = path.getFileSystem(new Configuration());
        if (fs.isFile(path)) {
            FSDataInputStream open = fs.open(path);
            try {
                props.load(open);
            } finally {
                IOUtils.closeStream(open);
            }
        }
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        props.setProperty(OozieClient.USER_NAME, ugi.getUserName());
        props.setProperty(OozieClient.GROUP_NAME, ugi.getGroupNames()[0]);

        Path qualified = fs.makeQualified(new Path(appPath));
        props.setProperty(context.pathKey(), qualified.toString());

        return props;
    }

    public static void main(String[] args) throws Exception {
        SimpleClient client = new SimpleClient(new OozieClient(args[0]));
        client.execute(args.length > 1 ? args[1] : null);
    }
}
