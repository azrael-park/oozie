package org.apache.oozie.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OozieClientIT {
    
    public static final Logger LOG = LoggerFactory.getLogger(OozieClientIT.class);
    
    protected final int POLLING = 1500;

    protected final String OOZIE_URL = "http://localhost:11000/oozie";

    protected static String user = "ndap";
    protected final static String group = "hadoop";
    protected static String nameNode = "hdfs://localhost:8020";
    protected static String jobTracker = "localhost:8032";
    protected static String hiveServer = "http://localhost:10000/default";
    protected final static String examplesRoot = "workflow-ndap";
    protected static String baseAppPath = nameNode + "/user/ndap/workflow-ndap/apps";
    protected static String definitionDir = "definitions";
    protected static String hadoopVersion = "hadoop-2";
    
    static {
        String host = "localhost";
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            host = inetAddress.getHostName();
        } catch (Exception e) {
            host = "localhost";
        }
        if (hadoopVersion.equals("hadoop-1")) {
            nameNode = "hdfs://" + host + ":9000";
            jobTracker = host + ":9001";
        } else if (hadoopVersion.equals("hadoop-2")) {
            nameNode = "hdfs://" + host + ":8020";
            jobTracker = host + ":8032";
        }
        hiveServer = "http://" + host + ":10000/default";
        String localUser = System.getProperty("user.name");
        user = localUser;
        baseAppPath = nameNode + "/user/" + user + "/" + examplesRoot + "/apps";
        definitionDir = OozieClientIT.class.getClassLoader().getResource("definitions").getPath();
    }
    
    protected void uploadApps(String appPath, String appName, String version) throws Exception {
        Path appDir = new Path(appPath);
        FileSystem fs = FileSystem.get(new URI(appPath), new Configuration());
        fs.mkdirs(appDir);
        
        File file = new File(definitionDir + "/" + version + "/" + appName + ".xml");
        Path src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/workflow.xml"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/workflow.xml");

        FSDataOutputStream fo = fs.create(new Path(appPath + "/job.properties"), true);
        String jobProperties = getJobProperties(version, appName);
        fo.write(jobProperties.getBytes());
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/job.properties");
    }

    protected void uploadApps(String appPath, String appName, String version, String extraFile) throws Exception {
        Path appDir = new Path(appPath);
        FileSystem fs = FileSystem.get(new URI(appPath), new Configuration());
        fs.mkdirs(appDir);

        File file = new File(definitionDir + "/" + version + "/" + appName + ".xml");
        Path src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/workflow.xml"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/workflow.xml");

        FSDataOutputStream fo = fs.create(new Path(appPath + "/job.properties"), true);
        String jobProperties = getJobProperties(version, appName);
        fo.write(jobProperties.getBytes());
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/job.properties");

        file = new File(definitionDir + "/" + version + "/"+extraFile);
        src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/"+extraFile));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/" + extraFile);
    }

    protected String getJobProperties(String version, String appName) throws Exception{
        String output = "";
        File file = new File(definitionDir + "/" + version + "/job.properties");
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = "";
        while ((line = br.readLine()) != null)  {
            if (line.startsWith("nameNode")) {
                line = "nameNode=" + nameNode;
            } else if (line.startsWith("jobTracker")) {
                line = "jobTracker=" + jobTracker;
            } else if (line.startsWith("hiveServer")) {
                line = "hiveServer=" + hiveServer;
            }
            output += line + "\n";
        }
        output += "user.name=" + user + "\n";
        output += "version=" + version + "\n";
        output += "appName=" + appName +"\n";
        return output;
    }
    
    protected String run(Properties props) throws Exception {
        if (props.get(OozieClient.APP_PATH) != null) {
            LOG.debug("[subimt app] " + props.getProperty(OozieClient.APP_PATH));
        } else if (props.get(OozieClient.COORDINATOR_APP_PATH) != null) {
            LOG.debug("[subimt coord] " + props.getProperty(OozieClient.COORDINATOR_APP_PATH));
        } else {
            LOG.debug("[subimt ] nothing");
        }
        LOG.trace("--- submit job properites start ---");
        for (Object key : props.keySet()) {
            LOG.trace(key + " = " + props.getProperty((String) key));
        }
        LOG.trace("--- submit job properites end ---");
        
        String id = getClient().run(props);
        LOG.info(">>>> run id >>> " + id);
        return id;
    }
    
    protected String monitorJob(String jobID) {
        String status = "";
        try {
            for (int i = 0; i < 50; i++) {
                WorkflowJob wfJob = getClient().getJobInfo(jobID);
                LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                List<WorkflowAction> actionList = wfJob.getActions();
                for (WorkflowAction action : actionList) {
                    LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                }
                status = wfJob.getStatus().toString();
                if (wfJob.getStatus().equals(WorkflowJob.Status.SUCCEEDED)
                        || wfJob.getStatus().equals(WorkflowJob.Status.KILLED)
                        || wfJob.getStatus().equals(WorkflowJob.Status.FAILED)) {
                    break;
                }
                Thread.sleep(POLLING);
            }
        } catch (Exception e) {
            LOG.debug("Fail to monitor : " + jobID, e);
        }
        return status;
    }
    
    protected WorkflowAction monitorFailedAction(String jobID) {
        WorkflowAction failedAction = null;
        try {
            for (int i = 0; i < 20; i++) {
                WorkflowJob wfJob = getClient().getJobInfo(jobID);
                LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                List<WorkflowAction> actionList = wfJob.getActions();
                for (WorkflowAction action : actionList) {
                    LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                    if (action.getStatus().equals(WorkflowAction.Status.START_MANUAL)) {
                        failedAction = action;
                        String log = getClient().getLog(failedAction.getId());
                        LOG.trace(log);
                        break;
                    }
                }
                if (failedAction != null) {
                    break;
                }
                if (wfJob.getStatus().equals(WorkflowJob.Status.SUCCEEDED)
                        || wfJob.getStatus().equals(WorkflowJob.Status.KILLED)
                        || wfJob.getStatus().equals(WorkflowJob.Status.FAILED)) {
                    break;
                }
                Thread.sleep(POLLING);
            }
        } catch (Exception e) {
            LOG.debug("Fail to monitor : " + jobID, e);
        }
        return failedAction;
    }
    
    protected Properties getDefaultProperties() {
        Properties configs = new Properties();
        configs.put(OozieClient.USER_NAME, user);
        configs.put(OozieClient.GROUP_NAME, group);
        
        configs.put("nameNode", nameNode);
        configs.put("jobTracker", jobTracker);
        configs.put("hiveServer", hiveServer);
        
        return configs;
    }
    
    protected OozieClient getClient() {
        OozieClient client = new OozieClient(OOZIE_URL);
        client.setDebugMode(1);
        return client;
    }
}
