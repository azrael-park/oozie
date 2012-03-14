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

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.oozie.client.WorkflowJob.Status.PREP;
import static org.apache.oozie.client.WorkflowJob.Status.RUNNING;
import static org.apache.oozie.client.WorkflowJob.Status.SUSPENDED;

public class SimpleClient {

    private static enum CONTEXT {
        WF{
            String pathKey() { return OozieClient.APP_PATH; }
            Map<String, Method> getJobProperties() { return WF_JOB_PROPS; }
            Map<String, Method> getActionProperties() { return WF_ACTION_PROPS; }
            List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getJobsInfo(pattern, start, length);
            }
            List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getActionsInfo(pattern, start, length);
            }
        },
        COORD{
            String pathKey() { return OozieClient.COORDINATOR_APP_PATH; }
            Map<String, Method> getJobProperties() { return COORD_JOB_PROPS; }
            Map<String, Method> getActionProperties() { return COORD_ACTION_PROPS; }
            List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getCoordJobsInfo(pattern, start, length);
            }
            List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                throw new UnsupportedOperationException("not-yet");
            }
        },
        BUNDLE{
            String pathKey() { return OozieClient.BUNDLE_APP_PATH; }
            Map<String, Method> getJobProperties() { return BUNDLE_JOB_PROPS; }
            Map<String, Method> getActionProperties() { return BUNDLE_ACTION_PROPS; }
            List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                return client.getBundleJobsInfo(pattern, start, length);
            }
            List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException {
                throw new UnsupportedOperationException("not-yet");
            }
        };

        abstract String pathKey();

        abstract Map<String, Method> getJobProperties();

        abstract Map<String, Method> getActionProperties();

        abstract List getJobsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException;

        abstract List getActionsInfo(OozieClient client, String pattern, int start, int length) throws OozieClientException;
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

    private static enum COMMAND {
        submit, start, run, rerun, kill, killall, suspend, resume, update, status, poll, cancel, log, jobs, actions, use, context, quit
    }

    CONTEXT context = CONTEXT.WF;
    OozieClient client;

    List<Polling> pollings = new ArrayList<Polling>();

    String jobID = null;

    SimpleClient(OozieClient client) {
        this.client = client;
    }

    public void execute(String appPath) throws Exception {

        if (appPath != null) {
            Properties props = jobDescription(appPath);
            jobID = client.run(props);
            System.out.println("[Runner/main] running job " + client.getJobInfo(jobID));
        }

        ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);

        SimpleCompletor completor = new SimpleCompletor(new String[0]);
        for (COMMAND command : COMMAND.values()) {
            completor.addCandidateString(command.name());
        }

        reader.addCompletor(completor);

        String line;
        while ((line = reader.readLine(context + ">")) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (jobID != null) {
                line.replaceAll("$$", jobID);
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
                    String newJobID = commands.length > 1 ? commands[1] : jobID;
                    client.reRun(newJobID, jobDescription(""));
                    System.out.println("rerunning job " + client.getJobInfo(newJobID) + (jobID == null ? "" : ", replacing " + jobID));
                    jobID = newJobID;
                    poll(newJobID);
                } else if (commands[0].equals("kill")) {
                    client.kill(jobID != null ? commands.length == 1 ? jobID : jobID + "@" + commands[1] : commands[1]);
                } else if (commands[0].equals("killall")) {
                    for (WorkflowJob job : client.getJobsInfo(commands.length > 1 ? commands[1] : "")) {
                        WorkflowJob.Status status = job.getStatus();
                        if (status.equals(PREP) || status.equals(RUNNING) || status.equals(SUSPENDED)) {
                            System.out.println("killing.. " + job.getId());
                            client.kill(job.getId());
                        }
                    }
                } else if (commands[0].equals("suspend")) {
                    client.suspend(jobID != null ? commands.length == 1 ? jobID : jobID + "@" + commands[1] : commands[1]);
                } else if (commands[0].equals("resume")) {
                    client.resume(jobID != null ? commands.length == 1 ? jobID : jobID + "@" + commands[1] : commands[1]);
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
                    client.getLog(jobID != null ? commands.length == 1 ? jobID : jobID + "@" + commands[1] : commands[1], System.out);
                } else if (commands[0].equals("jobs")) {
                    FilterParams params = new FilterParams(context.getJobProperties(), commands);
                    for (Object job : context.getJobsInfo(client, params.filter, params.start, params.length)) {
                        System.out.println(params.toString(job));
                    }
                } else if (commands[0].equals("actions")) {
                    FilterParams params = new FilterParams(context.getActionProperties(), commands);
                    for (WorkflowAction action : client.getActionsInfo(params.filter, params.start, params.length)) {
                        System.out.println(params.toString(action));
                    }
                } else if (commands[0].equals("use")) {
                    WorkflowJob job;
                    if (commands.length > 1 && commands[1].length() > 0 && Character.isLetter(commands[1].charAt(commands[1].length() - 1))) {
                        job = client.getJobInfo(commands[1]);
                    } else {
                        job = client.getJobsInfo("").get(commands.length > 1 ? Integer.parseInt(commands[1]) : 0);
                    }
                    System.out.println("set default job : " + job.getId() + (jobID == null ? "" : ", replacing " + jobID));
                    jobID = job.getId();
                } else if (commands[0].equals("quit")) {
                    break;
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
        }
    }

    private class FilterParams {

        String filter = "";
        int start = -1;
        int length = -1;
        Object[] props;

        FilterParams(Map<String, Method> maps, String[] command) {
            int i = 1;
            for (; i < command.length; i++) {
                if (!isNumeric(command[i])) {
                    if (filter.isEmpty() && command[i].contains("=")) {
                        filter = command[i];
                    } else {
                        break;
                    }
                } else {
                    if (start < 0) {
                        start = Integer.valueOf(command[i]);
                    } else if (length < 0) {
                        length = Integer.valueOf(command[i]);
                    } else {
                        break;
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
    }

    private String getStatus(String jobId) throws OozieClientException {
        StringBuilder builder = new StringBuilder();
        switch (context) {
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
        props.setProperty(context.pathKey(), appPath);

        return props;
    }

    public static void main(String[] args) throws Exception {
        SimpleClient client = new SimpleClient(new OozieClient(args[0]));
        client.execute(args.length > 1 ? args[1] : null);
    }
}
