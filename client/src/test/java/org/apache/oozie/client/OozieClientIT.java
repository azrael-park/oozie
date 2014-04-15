package org.apache.oozie.client;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OozieClientIT {
    
    public static final Logger LOG = LoggerFactory.getLogger(OozieClientIT.class);
    
    protected final int POLLING = 1500;

    protected final String OOZIE_URL = "http://localhost:11000/oozie";

    protected static String user = "ndap";
    protected final static String group = "user";
    protected final static String nameNode = "hdfs://localhost:9000";
    protected final static String jobTracker = "localhost:9001";
    protected final static String hiveServer = "http://localhost:10000/default";
    protected final static String examplesRoot = "workflow-ndap";
    protected static String baseAppPath = "hdfs://localhost:9000/user/ndap/workflow-ndap/apps";
    protected static String definitionDir = "definitions";
    
    static {
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
        
        file = new File(definitionDir + "/" + version + "/job.properties");
        src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/job.properties"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/job.properties");
        
    }

    protected void uploadCoordApps(String appPath, String appName, String version) throws Exception {

        Path appDir = new Path(appPath);
        FileSystem fs = FileSystem.get(new URI(appPath), new Configuration());
        fs.mkdirs(appDir);

        File file = new File(definitionDir + "/" + version + "/" + appName + ".xml");
        Path src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/coordinator.xml"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/coordinator.xml");

        file = new File(definitionDir + "/" + version + "/job.properties");
        src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/job.properties"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/job.properties");

    }

    protected void uploadBundleApps(String appPath, String appName, String version) throws Exception {

        Path appDir = new Path(appPath);
        FileSystem fs = FileSystem.get(new URI(appPath), new Configuration());
        fs.mkdirs(appDir);

        File file = new File(definitionDir + "/" + version + "/" + appName + ".xml");
        Path src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/bundle.xml"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/bundle.xml");

        file = new File(definitionDir + "/" + version + "/job.properties");
        src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/job.properties"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/job.properties");

    }

    protected void uploadAppsFrom(String appPath, String definitionFile, String version) throws Exception {

        Path appDir = new Path(appPath);
        FileSystem fs = FileSystem.get(new URI(appPath), new Configuration());
        fs.mkdirs(appDir);

        File file = new File(definitionDir + "/" + version + "/" + definitionFile);
        Path src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/workflow.xml"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/workflow.xml");

        file = new File(definitionDir + "/" + version + "/job.properties");
        src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/job.properties"));
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

        file = new File(definitionDir + "/" + version + "/job.properties");
        src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/job.properties"));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/job.properties");

        file = new File(definitionDir + "/" + version + "/"+extraFile);
        src = new Path(file.getAbsolutePath());
        fs.copyFromLocalFile(false, true, src, new Path(appPath + "/"+extraFile));
        LOG.trace("Copy " + file.getAbsolutePath() + " to " + appPath + "/" + extraFile);

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

    protected String runCoord(Properties props) throws Exception {

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

        configs.put("oozie.wf.workflow.notification.url", "http://localhost:8080/wf?jobId=$jobId&amp;status=$status");
        configs.put("oozie.wf.action.notification.url", "http://localhost:8080/action?jobId=$jobId&amp;ationId=$actionId&amp;" +
                "status=$status");
        configs.put("oozie.coord.action.notification.url", "http://localhost:8080/coord?actionId=$actionId&amp;status=$status");
        
        return configs;
    }
    
    protected OozieClient getClient() {
        OozieClient client = new OozieClient(OOZIE_URL);
        client.setDebugMode(1);
        return client;
    }
}
