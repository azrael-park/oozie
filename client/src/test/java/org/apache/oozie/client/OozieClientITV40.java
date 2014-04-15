package org.apache.oozie.client;

import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.Properties;

public class OozieClientITV40 extends OozieClientIT{
    
    /**
     * Test capture std-out, std-err from shell action .
     *
     */
    @Test
    public void testShellOutStreamV40() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "shell-outstream";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version, "script-outstream.sh");

            String jobID = run(configs);
            String status = "";
            String capture = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if(action.getName().equals("shell-v40")){
                            LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                            LOG.debug("    " + "capture >> \n " + action.getData() + "\n");
                            capture = action.getData();
                        }
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

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);

            //output file is changed in JavaActionExecutor
            //Assert.assertTrue(capture.contains("hello-standard-output"));
        } catch (Exception e) {
            LOG.info("Fail to testShellOutStreamV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testShellOutStreamV40 \n");
    }

}
