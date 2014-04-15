package org.apache.oozie;

import jline.ConsoleReader;
import jline.History;
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

import java.io.File;
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
    private static final String ACTION_ALL = "ALL";
    private static final String CURRENT_JOB_ID = "$CUR";

    private static enum COMMAND {
        submit { String help() { return "submit <app-path> : submit application"; } },
        start { String help() { return "start <job-id> : start submitted job and poll it"; } },
        run { String help() { return "run <app-path> : submit + start application"; } },
        rerun { String help() { return "rerun <job-id> : run the job again"; } },
        kill { String help() { return "kill <job-id> : kill the job"; } },
        killall { String help() { return "killall : kill all the jobs in non-terminal status"; } },
        suspend { String help() { return "suspend <job-id|action-id|action-name(with context)> : suspend the job or action"; } },
        resume { String help() { return "resume <job-id|action-id|action-name(with context)> : resume the job or action"; } },
        update { String help() { return "update <action-id|action-name(with context)> <attr-name=attr-value(,attr-name=attr-value)*> : update action spec. supported only for hive/el/decision action"; } },
        status { String help() { return "status <job-id> : displays status of the job"; } },
        poll { String help() { return "poll <job-id> : poll status of the job. ends when it goes to terminal status"; } },
        cancel { String help() { return "cancel <job-id> : cancels polling the job"; } },
        log { String help() { return "log <job-id|action-id|action-name(with context)> : get logs for the job/action"; } },
        data { String help() { return "data <action-id|action-name(with context)> : retrieves end data for the action"; } },
        xml { String help() { return "xml <job-id|action-id|action-name(with context)> : retrieves definition for the action"; } },
        jobs { String help() { return "jobs [-s start] [-l length] [-c] [-p] [-status status]: retrieves job list"; } },
        actions { String help() { return "actions [-s start] [-l length] [-c] [-p] [-status status]: retrieves action list"; } },
        use { String help() { return "use <job index> : set context job id"; } },
        failed { String help() { return "failed <job-id|action-id|action-name(with context)> : retrieves log URL for failed actions (only for monitored)"; } },
        context { String help() { return "context <job-id> : set context job id"; } },
        reset { String help() { return "reset : remove context job id"; } },
        version { String help() { return "version : shows current version of oozie"; } },
        queue { String help() { return "queue : dump executor queue"; } },
        def { String help() { return "def <job-id|action-id> : shows defintion of the job or action"; } },
        url { String help() { return "url : shows url of oozie server"; } },
        servers { String help() { return "list available Oozie servers"; }},
        quit { String help() { return "quit : quit the shell"; } };
        abstract String help();
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

        try {
            File userHome = new File(System.getProperty("user.home"));
            if (userHome.exists()) {
                reader.setHistory(new History(new File(userHome, ".ooziehistory")));
            }
        } catch (Exception e) {
            System.err.println("WARNING: Encountered an error while trying to initialize Hive's " +
                "history file.  History will not be available during this session.");
            System.err.println(e.getMessage());
        }

        SimpleCompletor completor = new SimpleCompletor(new String[0]);
        for (COMMAND command : COMMAND.values()) {
            completor.addCandidateString(command.name());
        }

        reader.addCompletor(completor);

        if (appPath != null) {
            executeCommand(appPath);
        }

        String line;
        while ((line = reader.readLine(context + (jobID != null ? ":" + idPart(jobID) : "") + ">")) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (!executeCommand(line)) {
                break;
            }
        }
    }

    private boolean executeCommand(String line) {
        if (jobID != null) {
            line.replaceAll(CURRENT_JOB_ID, jobID);
        }
        String[] commands = line.split("(\\s*,\\s*)|(\\s+)");
        try {
            if (commands[0].equals("help")) {
                if (commands.length > 1) {
                    for (int i = 1; i < commands.length; i++) {
                        try {
                            System.out.println(COMMAND.valueOf(commands[i].trim()).help());
                        } catch (IllegalArgumentException e) {
                            System.out.println("Invalid command " + commands[i].trim());
                        }
                    }
                } else {
                    for (COMMAND command : COMMAND.values()) {
                        System.out.println(command.help());
                    }
                }
            } else if (commands[0].equals("submit")) {
                String newJobID = client.submit(jobDescription(commands[1]));
                System.out.println("submitted job " + client.getJobInfo(newJobID) + (jobID == null ? "" : ", replacing " + jobID));
                jobID = newJobID;
            } else if (commands[0].equals("start")) {
                String jobID = getJobID(commands, 1);
                client.start(jobID);
                poll(jobID);
            } else if (commands[0].equals("run")) {
                String appPath = getPath(commands, 1);
                String newJobID = client.run(jobDescription(appPath));
                System.out.println("running job " + client.getJobInfo(newJobID) + (jobID == null ? "" : ", replacing " + jobID));
                jobID = newJobID;
                poll(newJobID);
            } else if (commands[0].equals("rerun")) {
                String runJobId = getJobID(commands, 1);
                String runAppPath = client.getJobInfo(runJobId).getAppPath();
                client.reRun(runJobId, jobDescription(runAppPath));
                System.out.println("rerunning job " + client.getJobInfo(runJobId) + (jobID == null ? "" : ", replacing " + jobID));
                jobID = runJobId;
                poll(runJobId);
            } else if (commands[0].equals("kill")) {
                client.kill(getID(commands, 1)[0]);
            } else if (commands[0].equals("killall")) {
                for (Object job : context.getJobsInfo(client, commands.length > 1 ? commands[1] : "", 1, 1000)) {
                    if (!context.isJobTerminal(job)) {
                        System.out.println("killing.. " + context.getJobId(job));
                        client.kill(context.getJobId(job));
                    }
                }
            } else if (commands[0].equals("suspend")) {
                client.suspend(getID(commands, 1)[0]);
            } else if (commands[0].equals("resume")) {
                client.resume(getID(commands, 1)[0]);
            } else if (commands[0].equals("update")) {
                String[] id = getID(commands, 1);
                String remain = line.substring(line.indexOf(commands[1]) + commands[1].length());
                client.update(id[0] + "@" + id[1], parseParams(remain));
            } else if (commands[0].equals("status")) {
                System.out.println(getStatus(getJobID(commands, 1)));
            } else if (commands[0].equals("poll")) {
                poll(getJobID(commands, 1));
            } else if (commands[0].equals("cancel")) {
                if (pollings.size() > 0) {
                    pollings.get(0).cancel = true;
                    pollings.get(0).join();
                }
            } else if (commands[0].equals("log")) {
                String[] id = getID(commands, 1);
                if (id[1] == null) {
                    client.getLog(id[0], System.out);
                } else {
                    client.getLog(id[0] + "@" + id[1], System.out);
                }
            } else if (commands[0].equals("xml")) {
                String[] id = getID(commands, 1);
                if (id[1] != null) {
                    if (id[1].equals(ACTION_ALL)) {
                      WorkflowJob job = client.getJobInfo(id[0]);
                      for (WorkflowAction action : job.getActions()) {
                        System.out.println("---------------------- " + action.getName() + " ----------------------");
                        System.out.println(XmlUtils.prettyPrint(action.getConf()).toString());
                      }
                    } else {
                      WorkflowAction action = client.getWorkflowActionInfo(id[0] + "@" + id[1]);
                      System.out.println("---------------------- " + action.getName() + " ----------------------");
                      System.out.println(XmlUtils.prettyPrint(action.getConf()).toString());
                    }
                } else {
                    System.out.println(XmlUtils.prettyPrint(context.getJobConf(context.getJobInfo(client, id[0]))));
                }
            } else if (commands[0].equals("data")) {
                String[] id = getID(commands, 1);
                if (id[1] != null && id[1].equals(ACTION_ALL)) {
                    WorkflowAction action = client.getWorkflowActionInfo(id[0]);
                    if (action.getData() != null && !action.getData().isEmpty()) {
                        System.out.println("---------------------- " + action.getName() + " ----------------------");
                        System.out.println(action.getData());
                    }
                } else {
                    WorkflowJob job = client.getJobInfo(id[0]);
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
                String targetID = params.jobID != null ? params.jobID : jobID;
                if (params.parent && context == CONTEXT.COORD) {
                    if (targetID == null) {
                        System.out.println("coordinator id is not set");
                        return true;
                    }
                    System.out.println("Workflows originated from coordinator job " + targetID);
                    for (WorkflowJob child : client.getJobsForCoord(targetID)) {
                        System.out.println(params.toString(child));
                    }
                    return true;
                }
                if (!params.all && params.jobID != null) {
                    params.appendFilter("id=" + params.jobID);
                }
                for (int i=0 ; i <commands.length; i++) {
                    if (commands[i].equals("-status")) {
                        int j = i+1;
                        params.appendFilter("status=" + commands[j]);
                    }
                }
                jobIDs.clear();
                int index = 0;
                for (Object job : context.getJobsInfo(client, params.filter, params.start, params.length)) {
                    StringBuilder pendingActions = new StringBuilder();
                    boolean noActionStart = true;
                    if (job instanceof WorkflowJob) {
                        WorkflowJob workflow = client.getJobInfo(((WorkflowJob)job).getId());
                        for (WorkflowAction wfAction : workflow.getActions()) {
                            if (wfAction.getStatus() == WorkflowAction.Status.START_MANUAL ||
                                wfAction.getStatus() == WorkflowAction.Status.END_MANUAL) {
                                if (pendingActions.length() > 0) {
                                    pendingActions.append(',');
                                }
                                pendingActions.append(wfAction.getName());
                            }
                            if (wfAction.getStatus() != WorkflowAction.Status.PREP) {
                                noActionStart = false;
                            }
                        }
                    }
                    StringBuffer actionsStatus = new StringBuffer();
                    if (pendingActions.length() > 0 ) {
                        actionsStatus.append(" pending-on[" + pendingActions + "] ");
                    }
                    if (noActionStart) {
                        actionsStatus.append(" no-start-action");
                    }
                    System.out.printf("[%1$2d] %2$s\n", index++, params.toString(job, actionsStatus.toString()));
                    jobIDs.add(context.getJobId(job));
                    if (params.dumpXML) {
                        System.out.println(XmlUtils.prettyPrint(context.getJobConf(job)).toString());
                    }
                }
            } else if (commands[0].equals("actions")) {
                FilterParams params = new FilterParams(context.getActionProperties(), commands);
                CONTEXT context = params.getContext();
                if (!params.all && (params.jobID != null || jobID != null)) {
                    params.appendFilter("wfId=" + (params.jobID != null ? params.jobID : jobID));
                }
                for (int i=0 ; i <commands.length; i++) {
                    if (commands[i].equals("-status")) {
                        int j = i+1;
                        params.appendFilter("status=" + commands[j]);
                    }
                }
                List actions = context.getActionsInfo(client, params.filter, params.start, params.length);
                for (Object action : actions) {
                    System.out.println(params.toString(action));
                    if (params.dumpXML) {
                        System.out.println(XmlUtils.prettyPrint(context.getActionConf(action)).toString());
                    }
                }
            } else if (commands[0].equals("failed")) {
                String[] id = getID(commands, 1);
                Map<String, List<String>> result = client.getFailedTaskURLs(id[0] + "@" + id[1]);
                for (Map.Entry<String, List<String>> entry : result.entrySet()) {
                    System.out.println(entry.getKey());
                    for (String value : entry.getValue()) {
                        System.out.println("    " + value);
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
            } else if (commands[0].equals("reset")) {
                jobID = null;
            } else if (commands[0].equals("version")) {
                Map<String, String> result = client.getServerBuildInfo();
                for (Map.Entry<String, String> entry : result.entrySet()) {
                  System.out.println(entry);
                }
            } else if (commands[0].equals("quit")) {
                return false;
            } else if (commands[0].equals("context")) {
                CONTEXT newContext;
                try {
                    newContext = CONTEXT.valueOf(commands[1].toUpperCase());
                } catch (IllegalArgumentException e) {
                    System.out.println("supports " + Arrays.toString(CONTEXT.values()) + " only");
                    return true;
                }
                if (context != newContext) {
                    context = newContext;
                    jobID = null;
                }
            } else if (commands[0].equals("queue")) {
                List<String> dump = client.getQueueDump();
                if (dump != null) {
                    System.out.println(dump);
                }
            } else if (commands[0].equals("def")) {
                String[] id = getID(commands, 1);
                String definition = client.getJobDefinition(id[0]);
                if (definition != null) {
                    if (id[1] == null) {
                        System.out.println(XmlUtils.prettyPrint(definition));
                    } else {
                        System.out.println(XmlUtils.prettyPrint(definition, "action", "name", id[1]));
                    }
                }
            } else if (commands[0].equals("url")) {
                System.out.println(client.getOozieUrl());
            } else if (commands[0].equals("servers")) {
                Map<String, String> servers = client.getAvailableOozieServers();
                for (Map.Entry entry: servers.entrySet()) {
                    System.out.println(entry.getKey() + " : " + entry.getValue());
                }
            } else {
                System.err.println("invalid command " + line);
            }
        } catch (IllegalArgumentException e) {
            System.err.println("invalid argument.. " + e.getMessage());
            System.out.println(COMMAND.valueOf(commands[0].trim()).help());
        } catch (OozieClientException e) {
            e.printStackTrace();
            if (e.getDetail() != null) {
                System.out.println(e.getDetail().replaceAll("\\|", "\n\t"));
            }
        } catch (Exception e) {
          e.printStackTrace();
        }
        return true;
    }

    private String getPath(String[] commands, int start) {
        if (commands.length > start) {
            return commands[start];
        }
        throw new IllegalArgumentException("path is missing");
    }

    private String getJobID(String[] commands, int start) {
        if (commands.length > start) {
            if (isJobID(commands[start])) {
                return commands[start];
            }
        }
        if (jobID == null) {
            throw new IllegalArgumentException("jobId is missing");
        }
        return jobID;
    }

    private String[] getID(String[] commands, int start) {
        if (commands.length > start) {
            if (isJobID(commands[start])) {
                return new String[] { commands[start], null};
            }
            if (isActionID(commands[start])) {
                int index = commands[start].indexOf('@');
                return new String[] {commands[start].substring(0, index), commands[start].substring(index + 1)};
            }
            if (jobID != null) {
                return new String[] {jobID, commands[1]};
            }
        }
        if (jobID == null) {
            throw new IllegalArgumentException("(job/action)Id is missing");
        }
        return new String[] {jobID, null};
    }

    private static Pattern JOB_ID_PATTERN = Pattern.compile("\\d{7}-\\d{15}-\\S+-\\S+-[WCB]");
    private static Pattern ACTION_ID_PATTERN = Pattern.compile(JOB_ID_PATTERN + "@\\S+");

    private static boolean isJobID(String string) {
        return JOB_ID_PATTERN.matcher(string).matches();
    }

    private static boolean isActionID(String string) {
        return ACTION_ID_PATTERN.matcher(string).matches();
    }

    private static String idPart(String jobID) {
        return jobID.substring(0, jobID.indexOf('-', jobID.indexOf('-') + 1));
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
        boolean parent;
        boolean count;
        boolean all;
        String filter = "";
        String jobID;
        String actionID;
        int start = -1;
        int length = -1;
        Object[] props;

        FilterParams(Map<String, Method> maps, String[] command) {
            List<String> column = new ArrayList<String>();
            for (int i = 1; i < command.length; i++) {
                if (command[i].equals("-x") || command[i].equals("--xml")) {
                    dumpXML = true;
                } else if (command[i].equals("-p") || command[i].equals("--parent")) {
                    parent = true;
                } else if (command[i].equals("-c") || command[i].equals("--count")) {
                    count = true;
                } else if (command[i].equals("-a") || command[i].equals("--all")) {
                    all = true;
                } else if (command[i].equals("-s") || command[i].equals("--start")) {
                    start = Integer.valueOf(command[++i]);
                } else if (command[i].equals("-l") || command[i].equals("--length")) {
                    length = Integer.valueOf(command[++i]);
                } else if (isJobID(command[i])) {
                    jobID = command[i];
                } else if (isActionID(command[i])) {
                    actionID = command[i];
                } else if (command[i].contains("=")) {
                    appendFilter(command[i]);
                } else {
                    column.add(command[i]);
                }
            }
            if (count && (dumpXML || parent || start >= 0 || length >= 0)) {
                throw new IllegalArgumentException("-s, -l, -p and -x options are not allowed with -c option");
            }
            props = accessor(maps, column);
        }

        public void appendFilter(String append) {
            filter = filter.isEmpty() ? append : filter + ";" + append;
        }

        private Object[] accessor(Map<String, Method> maps, List<String> props) {
            List<Object> methods = new ArrayList<Object>();
            for (String key : props) {
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
            return toString(instance, null);
        }

        private String toString(Object instance, String append) throws Exception {
            StringBuilder builder = new StringBuilder(instance.toString());
            for (int i = 0; i < props.length;) {
                builder.append(", ").append(props[i++]).append('=').append(((Method) props[i++]).invoke(instance));
            }
            if (append != null) {
                builder.append(append);
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
                        int index = 0;
                        String prevQueryID = null;
                        for (HiveStatus status : client.getHiveStatusListForActionID(action.getId())) {
                            builder.append("   ");
                            if (prevQueryID == null || !prevQueryID.equals(status.getQueryId())) {
                                prevQueryID = status.getQueryId();
                                index++;
                            }
                            builder.append('#').append(index).append(' ');
                            builder.append(status.getStageId()).append("-->");
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
            setDaemon(true);
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
            param = param.trim();
            int eq = param.indexOf('=');
            String key = eq < 0 ? param : param.substring(0, eq).trim();
            String value = eq < 0 ? null : param.substring(eq + 1).trim();
            params.put(key, value);
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
        String key = (context == null ? CONTEXT.WF : context).pathKey();
        props.setProperty(key, qualified.toString());

        return props;
    }

    public static void main(String[] args) throws Exception {
        SimpleClient client = new SimpleClient(new OozieClient(args[0]));
        client.execute(args.length > 1 ? args[1] : null);
    }
}
