package org.apache.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.oozie.client.HiveStatus;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.oozie.client.WorkflowJob.Status.PREP;
import static org.apache.oozie.client.WorkflowJob.Status.RUNNING;
import static org.apache.oozie.client.WorkflowJob.Status.SUSPENDED;

public class SimpleClient {

    OozieClient client;

    SimpleClient(OozieClient client) {
        this.client = client;
    }

    public void execute(String appPath) throws Exception {
        String jobID = null;

        if (appPath != null) {
            Properties props = jobDescription(appPath);
            jobID = client.submit(props);
            System.err.println("[Runner/main] submitted job " + client.getJobInfo(jobID));
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

        String prev = null;
        String line;
        for (System.out.print('>');(line = reader.readLine()) != null;System.out.print('>')) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            if (prev != null && line.equals("!")) {
                line = prev;
            }
            try {
                String[] commands = line.split("[\\s]+");
                if (commands[0].equals("submit")) {
                    String newJobID = client.submit(jobDescription(commands[1]));
                    System.err.println("submitted job " + client.getJobInfo(newJobID) + (jobID == null ? "" : ", replacing " + jobID));
                    jobID = newJobID;
                } else if (commands[0].equals("start")) {
                    client.start(jobID != null ? jobID : commands[1]);
                } else if (commands[0].equals("kill")) {
                    client.kill(jobID != null ? jobID : commands[1]);
                } else if (commands[0].equals("killall")) {
                    for (WorkflowJob job : client.getJobsInfo(commands.length > 1 ? commands[1] : "")) {
                        WorkflowJob.Status status = job.getStatus();
                        if (status.equals(PREP) || status.equals(RUNNING) || status.equals(SUSPENDED)) {
                            System.err.println("killing.. " + job.getId());
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
                    WorkflowJob wflow = client.getJobInfo(jobID != null ? jobID : commands[1]);
                    System.out.println(wflow.getId() + ":" + wflow.getStatus());
                    for (WorkflowAction action : wflow.getActions()) {
                        System.out.println(action.toString());
                        if (action.getType().equals("hive")) {
                            for (HiveStatus status : client.getHiveStatusListForActionID(action.getId())) {
                                System.out.println("   " + status.getStageId() + "-->" + status.getJobId() + ":" + status.getStatus());
                            }
                        }
                    }
                } else if (commands[0].equals("log")) {
                    client.getLog(jobID != null ? commands.length == 1 ? jobID : jobID + "@" + commands[1] : commands[1], System.out);
                } else if (commands[0].equals("jobs")) {
                    for (WorkflowJob job : client.getJobsInfo(commands.length > 1 ? commands[1] : "")) {
                        System.out.println(job.toString());
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
                } else {
                    System.out.println("invalid command " + line);
                }
                prev = line;
            } catch (Exception e) {
                e.printStackTrace();
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
        props.setProperty("user.name", "navis");
        props.setProperty("group.name", "nexr");
        props.setProperty("oozie.wf.application.path", appPath);

        return props;
    }

    public static void main(String[] args) throws Exception {
        SimpleClient client = new SimpleClient(new OozieClient(args[0]));
        client.execute(args.length > 1 ? args[1] : null);
    }
}
