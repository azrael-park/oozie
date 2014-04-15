/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.client;

import junit.framework.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

public class OozieClientITV31 extends OozieClientIT{
    
    public static final Logger LOG = LoggerFactory.getLogger(OozieClientITV31.class);

    /**
     * Test impersonation interact with hadoop using java action
     * 
     */
    @Test
    public void testRunWithImpersonationV31() {
        try {
            Properties configs = getDefaultProperties();
            configs.put(OozieClient.USER_NAME, "ndap-test");
            
            // ehco
            String appName = "shell";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            
            uploadApps(appPath, appName, version);
            
            String jobID = run(configs);
            LOG.debug("\n ----- Job Log ---------------\n");
            LOG.debug(getClient().getJobLog(jobID) + "\n");
            LOG.debug("----- Job Log  end ----------");

            WorkflowAction failedAction = monitorFailedAction(jobID);
            Assert.assertNotNull(failedAction);
//            String failLog = getClient().getLog(failedAction.getId());
//
//            LOG.debug("----- failed Action Log ---------------\n");
//            LOG.debug(failLog + "\n");
//            LOG.debug("----- failed Action Log  end ----------");
//
//            Assert.assertTrue(failLog.contains("Permission denied: user=ndap-test"));
            
            // clear resource
            getClient().kill(jobID);
        } catch (Exception e) {
            LOG.info("Fail to testRunWithImpersonation", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testRunWithImpersonation \n");
    }
    
    /**
     * Test hiveserver2 Custom Authentication. It needs to turn on custom authentication on hiveserver2.
     * 
     */
    @Ignore
    @Test
    public void testHiveServerAuthV31() {
        try {
            
            Properties configs = getDefaultProperties();
            
            String user = "hive";
            String passwd = "hiveserver";
            configs.put(OozieClient.USER_NAME, "hive");
            
            String address = configs.getProperty(hiveServer);
            address += "/default;user=" + user + ";password=" + passwd;
            // address += ";user="+user+";password="+passwd;
            
            LOG.debug("hiveServer ::: " + address);
            configs.put(hiveServer, address);
            
            // ehco
            String appName = "shell";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            
            uploadApps(appPath, appName, version);
            
            String jobID = run(configs);
            WorkflowAction failedAction = monitorFailedAction(jobID);
//            Assert.assertNotNull(failedAction);
//            String failLog = getClient().getLog(failedAction.getId());
//
//            LOG.debug("failed Action Log >>> \n" + failLog);
//
//            Assert.assertTrue(failLog.contains("Authentication Fail"));
            
            // clear resource
            getClient().kill(jobID);
        } catch (Exception e) {
            LOG.info("Fail to testHiveServerAuth", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testHiveServerAuth \n");
    }
    
    /**
     * Test very simple shell action .
     * 
     */
    @Test
    public void testShellV31() {
        try {
            Properties configs = getDefaultProperties();
            
            // ehco
            String appName = "shell";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            
            uploadApps(appPath, appName, "v31");
            
            String jobID = run(configs);
            String status = monitorJob(jobID);
            
            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
            
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);

            WorkflowAction shellAction = null;
            WorkflowJob wfJob = getClient().getJobInfo(jobID);
            List<WorkflowAction> actionList = wfJob.getActions();
            for (WorkflowAction action : actionList) {
                if (action.getName().equals("shell")) {
                    shellAction = action;
                }
            }
            Assert.assertNotNull(shellAction);
            LOG.debug(" ---- JOB LOG ----");
            LOG.debug(getClient().getJobLog(jobID));
            LOG.debug(" ---- JOB LOG end ----");

//            LOG.debug(" ---- Action LOG ----");
//            LOG.debug(getClient().getLog(shellAction.getId()));
//            LOG.debug(" ---- Action LOG end ----");


        } catch (Exception e) {
            LOG.info("Fail to testShellV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testShellV31 \n");
    }

    /**
     * Test retry-max, retry-interval of action attribute.
     * Set the following configs on oozie-site.xml :
     * <ul>
     *     <li>oozie.service.LiteWorkflowStoreService.user.retry.error.code.ext=ALL</li>
     *     <li>oozie.service.LiteWorkflowStoreService.user.retry.suspend=true</li>
     * </ul>
     * The shell action included invalid command finished mr job gracefully, but isMainSuccessful is <code>false</code>.
     * If retry-max is greater than 1, it cause the USER_RETRY from ActionEndXCommand#handleError.
     * If retry-max is 0, then it cause the ERROR from ActionEndXCommand#handleError and then KILLED.
     *
     */
    @Test
    public void testRetryShellV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "retry-shell";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);

            String status = "";
            WorkflowAction shell = null;
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                        if(action.getName().equals("shell")){
                            shell = action;
                            if(shell.getStatus() == WorkflowAction.Status.USER_RETRY){
                                break;
                            }
                        }
                    }
                    status = wfJob.getStatus().toString();
                    if (wfJob.getStatus().equals(WorkflowJob.Status.SUCCEEDED)
                            || wfJob.getStatus().equals(WorkflowJob.Status.KILLED)
                            || wfJob.getStatus().equals(WorkflowJob.Status.FAILED)) {
                        break;
                    }
                    if(shell !=null && shell.getStatus() == WorkflowAction.Status.USER_RETRY){
                        break;
                    }
                    Thread.sleep(POLLING);
                }
            } catch (Exception e) {
                LOG.debug("Fail to monitor : " + jobID, e);
            }


            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowAction.Status.USER_RETRY, shell.getStatus());

        } catch (Exception e) {
            LOG.info("Fail to testRetryShellV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testRetryShellV31 \n");
    }

    /**
     * Test the killing job when action is in USER_RETRY.
     * Set the following configs on oozie-site.xml :
     * <ul>
     *     <li>oozie.service.LiteWorkflowStoreService.user.retry.error.code.ext=ALL</li>
     *     <li>oozie.service.LiteWorkflowStoreService.user.retry.suspend=true</li>
     * </ul>
     *
     */
    @Test
    public void testUserRetryKillV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "retry-shell";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);

            String status = "";
            WorkflowJob wfJob = null;
            WorkflowAction shell = null;
            try {
                for (int i = 0; i < 50; i++) {
                    wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                        if(action.getName().equals("shell")){
                            shell = action;
                            if(shell.getStatus() == WorkflowAction.Status.USER_RETRY){
                                break;
                            }
                        }
                    }
                    status = wfJob.getStatus().toString();
                    if (wfJob.getStatus().equals(WorkflowJob.Status.SUCCEEDED)
                            || wfJob.getStatus().equals(WorkflowJob.Status.KILLED)
                            || wfJob.getStatus().equals(WorkflowJob.Status.FAILED)) {
                        break;
                    }
                    if(shell !=null && shell.getStatus() == WorkflowAction.Status.USER_RETRY){
                        break;
                    }
                    Thread.sleep(POLLING);
                }
            } catch (Exception e) {
                LOG.debug("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowAction.Status.USER_RETRY, shell.getStatus());
            Assert.assertEquals(WorkflowJob.Status.RUNNING, wfJob.getStatus());

            Thread.sleep(1000);

            LOG.info("------ kill job ------");
            getClient().kill(jobID);

            try {
                for (int i = 0; i < 10; i++) {
                    wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                        if(action.getName().equals("shell")){
                            shell = action;
                            if(shell.getStatus() == WorkflowAction.Status.USER_RETRY){
                                break;
                            }
                        }
                    }
                    status = wfJob.getStatus().toString();
                    if (wfJob.getStatus().equals(WorkflowJob.Status.SUCCEEDED)
                            || wfJob.getStatus().equals(WorkflowJob.Status.KILLED)
                            || wfJob.getStatus().equals(WorkflowJob.Status.FAILED)) {
                        break;
                    }
                    if(shell !=null && shell.getStatus() == WorkflowAction.Status.USER_RETRY){
                        break;
                    }
                    Thread.sleep(POLLING);
                }
            } catch (Exception e) {
                LOG.debug("Fail to monitor : " + jobID, e);
            }

            Assert.assertEquals(WorkflowAction.Status.KILLED, shell.getStatus());
            Assert.assertEquals(WorkflowJob.Status.KILLED, wfJob.getStatus());

        } catch (Exception e) {
            LOG.info("Fail to testRetryShellV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testRetryShellV31 \n");
    }

    /**
     * Test hive action with NON_TRANSIENT setting.
     * See, oozie-site.xml.
     * <br/>
     * If NON_TRANSIENT=Exception, the hive action with invalid query result in START_RETRY
     * from ActionStartXCommand#handleNonTransient, otherwise DONE from ActionStartXCommand#handleError
     * and then END_MANUAL from ActionEncXCommand#onFailure consequently.
     *
     */
    @Test
    public void testHiveWithNonTransientV31() {
        try {
            Properties configs = getDefaultProperties();

            // hive actions in which query occurs error.
            String appName = "retry-hive";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);

            WorkflowAction action = monitorFailedAction(jobID);
            Assert.assertEquals("hive-start-manual", action.getName());

//            String log = getClient().getLog(action.getId());
//            LOG.info("action log >>>>>>>>>> \n" + log + "\n >>>>>>>>>>");

            LOG.info("action status : " + action.getStatus().toString());
            Assert.assertEquals(WorkflowAction.Status.START_MANUAL.toString(), action.getStatus().toString());

        } catch (Exception e) {
            LOG.info("Fail to testHiveWithNonTransientV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testHiveWithNonTransientV31 \n");
    }

    /**
     * Test kill and rerun immediately. It need to check oozie.log not to occur Exception manually.
     *
     */
    @Ignore
    @Test
    public void testShellKillRerunV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "shell-3";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            configs.put("oozie.wf.rerun.failnodes","true");

            uploadApps(appPath, appName, version);

            String jobID = run(configs);

            for(int i=0; i<50; i++){
                Thread.sleep(1000);
                getClient().kill(jobID);
                Thread.sleep(1000);
                getClient().reRun(jobID, configs);

            }

            String status = monitorJob(jobID);

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
        } catch (Exception e) {
            LOG.info("Fail to testShellKillRerunV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testShellKillRerunV31 \n");
    }

    /**
     * Test negative case. Reruns the running job .
     *
     */
    @Test
    public void testRerunRunningJobV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "shell-3";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            configs.put("oozie.wf.rerun.failnodes","true");

            uploadApps(appPath, appName, version);

            String jobID = run(configs);

            Thread.sleep(1000);


            getClient().reRun(jobID,configs);

        } catch (Exception e) {

            Assert.assertTrue(e instanceof OozieClientException);

            OozieClientException oe = (OozieClientException)e;
            Assert.assertEquals("E0805",oe.getErrorCode());
        }
        LOG.info("    >>>> Pass testRerunRunningJob \n");
    }
    
    /**
     * Test execution Path Error during FORK-JOIN
     * 
     */
    @Test
    public void testForkJoin1V31() {
        
        try {
            Properties configs = getDefaultProperties();
            
            // simple hive actions in fork-join
            String appName = "fork-join1";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            
            uploadApps(appPath, appName, version);
            
            String jobID = run(configs);
            String status = monitorJob(jobID);
            
            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
            
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
        } catch (Exception e) {
            LOG.info("Fail to testForkJoin1V31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testForkJoin1V31 \n");
    }
    
    /**
     * Test execution Path Error during FORK-JOIN but it does not occur.
     * 
     */
    @Test
    public void testForkJoin2V31() {
        
        try {
            Properties configs = getDefaultProperties();
            
            // simple hive actions in fork-join
            String appName = "fork-join2";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            
            uploadApps(appPath, appName, version);
            
            String jobID = run(configs);
            String status = monitorJob(jobID);
            
            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
            
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
        } catch (Exception e) {
            LOG.info("Fail to testForkJoin2V31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testForkJoin2V31 \n");
    }
    
    /**
     * Test capture-out in shell action.
     *
     */
    @Test
    public void testShellCaptureOutV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "shell";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);
            String jobID = run(configs);
            String status = "";
            String capture = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if (action.getName().equals("shell")) {
                            LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                            LOG.debug("    " + "capture -- " + " [" + action.getData() + "]");
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

            Assert.assertTrue(capture.contains("workflow-test"));
        } catch (Exception e) {
            LOG.info("Fail to testShellCaptureOutV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testShellCaptureOutV31 \n");
    }

    /**
     * Test <code>capture-output dump=true</code> from shell action .
     * It needs to search <code>stdout dump-xxx hello-standard-output</code> in oozie.log manually if dump=true.
     *
     */
    @Test
    public void testShellOutStreamV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "shell-outstream";
            String version = "v31";
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
                        if (action.getName().equals("shell")) {
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

            Assert.assertTrue(capture.contains("hello-standard-output"));
        } catch (Exception e) {
            LOG.info("Fail to testShellOutStreamV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testShellOutStreamV31 \n");
    }

    /**
     * Manual Test.
     * <br/>
     * Test <code>capture-error dump=true</code> from shell action.
     * If <code>dump=true</code>, stderr is written in oozie.log but not stored in actionData.
     * It needs to search <code>stderr dump-xxx hello-standard-error</code> in oozie.log manually.
     *
     */
    @Test
    public void testShellErrStreamV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "shell-errstream";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version, "script-errstream.sh");

            String jobID = run(configs);
            String status = "";
            String capture = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if (action.getName().equals("shell")) {
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

            Assert.assertEquals(WorkflowJob.Status.KILLED.toString(), status);

        } catch (Exception e) {
            LOG.info("Fail to testShellErrStreamV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testShellErrStreamV31 \n");
    }

    /**
     * Test standard out and standard err of Custom Java ActionMain - JavaMainTest.
     * </p> standard out
     * <li>1. run test case and pass</li>
     * </p> standard err
     * <li>1. set "echooo" for command.</li>
     * <li>2. run test case and fail</li>
     * <li>3. search in oozie.log and find <code>'stderr dump-xxx javamain-hello-error-azrael'</code> manually</li>
     *
     */
    @Test
    public void testJavaMainV31() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "java-main";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);
            String status = "";
            String capture = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if (action.getName().equals("java1")) {
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

            Assert.assertTrue(capture.contains("hello-azrael="));

        } catch (Exception e) {
            LOG.info("Fail to testJavaMainV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testJavaMainV31 \n");
    }

    /**
     * Test hive action include mr job.
     *
     */
    @Test
    public void testHiveMRV31() {
        try {
            Properties configs = getDefaultProperties();

            // hive actions of which query include count(*)
            String appName = "hive-mr";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);
            String status = monitorJob(jobID);

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
        } catch (Exception e) {
            LOG.info("Fail to testHiveMRV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testHiveMRV31 \n");
    }

    /**
     * Test hive action include multiple queries.
     *
     */
    @Test
    public void testHiveMultiQueryV31() {
        try {
            Properties configs = getDefaultProperties();

            // hive actions of which query include count(*)
            String appName = "hive-multi-query";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);
            String status = monitorJob(jobID);
            status = monitorJob(jobID);

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
        } catch (Exception e) {
            LOG.info("Fail to testHiveMultiQueryV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testHiveMultiQueryV31 \n");
    }

//    /**
//     * Test hive action of which query include hangle. Need manual test to check query result.
//     *
//     */
//    @Test
//    public void testHiveHangleV31() {
//        try {
//
//            String dataDir = getClass().getClassLoader().getResource("data/files").getPath();
//            String dataFile = dataDir + "/hangle.txt";
//
//            Properties configs = getDefaultProperties();
//
//            // hive actions of which query include hangle
//            String appName = "hive-hangle";
//            String version = "v31";
//            String appPath = baseAppPath + "/" + version + "/" + appName;
//            configs.put(OozieClient.APP_PATH, appPath);
//            configs.put("appName", appName);
//            configs.put("version", version);
//            configs.put("data_file", dataFile);
//
//            uploadApps(appPath, appName, version);
//
//            String jobID = run(configs);
//            String status = monitorJob(jobID);
//
//            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
//
//            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
//
//            WorkflowAction hangleAction = null;
//            WorkflowJob wfJob = getClient().getJobInfo(jobID);
//            List<WorkflowAction> actionList = wfJob.getActions();
//            for (WorkflowAction action : actionList) {
//                if (action.getName().equals("hive-hangle")) {
//                    hangleAction = action;
//                }
//            }
//            Assert.assertNotNull(hangleAction);
//            LOG.debug(" ---- Action LOG ----");
//            String actionLog = getClient().getActionLog(hangleAction.getId());
//            LOG.debug(actionLog);
//            LOG.debug(" ---- Action LOG end ----");
//
//            Assert.assertTrue(actionLog.contains("etch dump\n2\n"));
//        } catch (Exception e) {
//            LOG.info("Fail to testHiveHangleV31", e);
//            Assert.fail();
//        }
//        LOG.info("    >>>> Pass testHiveHangleV31 \n");
//    }
    
//    /**
//     * Test update query during start_manual status.
//     *
//     */
//    @Test
//    public void testHiveUpdateQueryV31() {
//        try {
//            Properties configs = getDefaultProperties();
//
//            // hive actions in which query occurs error.
//            String appName = "hive-start-manual";
//            String version = "v31";
//            String appPath = baseAppPath + "/" + version + "/" + appName;
//            configs.put(OozieClient.APP_PATH, appPath);
//            configs.put("appName", appName);
//            configs.put("version", version);
//
//            uploadApps(appPath, appName, version);
//
//            String jobID = run(configs);
//
//            WorkflowAction action = monitorFailedAction(jobID);
//            Assert.assertEquals("hive-start-manual", action.getName());
//            LOG.info("action status : " + action.getStatus().toString());
//
//            String log = getClient().getLog(action.getId());
//            LOG.info("action log >>>>>>>>>> \n" + log + "\n >>>>>>>>>>");
//
//            // update query and resume action
//            String newQuery = "SHOW TABLES";
//
//            Map<String, String> updates = new HashMap<String, String>();
//            updates.put("query", newQuery);
//            getClient().update(action.getId(), updates);
//
//            Thread.sleep(1000);
//            getClient().resume(action.getId());
//
//            Thread.sleep(1000);
//            String status = monitorJob(jobID);
//
//            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
//
//            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
//        } catch (Exception e) {
//            LOG.info("Fail to testHiveUpdateQueryV31", e);
//            Assert.fail();
//        }
//        LOG.info("    >>>> Pass testHiveUpdateQueryV31 \n");
//    }
    
//    /**
//     * Test update multi query during start_manual status.
//     *
//     */
//    @Test
//    public void testHiveUpdateQueriesV31() {
//        try {
//            Properties configs = getDefaultProperties();
//
//            // hive actions in which query occurs error.
//            String appName = "hive-start-manual";
//            String version = "v31";
//            String appPath = baseAppPath + "/" + version + "/" + appName;
//            configs.put(OozieClient.APP_PATH, appPath);
//            configs.put("appName", appName);
//            configs.put("version", version);
//
//            uploadApps(appPath, appName, version);
//
//            String jobID = run(configs);
//
//            WorkflowAction action = monitorFailedAction(jobID);
//            Assert.assertEquals("hive-start-manual", action.getName());
//            LOG.info("action status : " + action.getStatus().toString());
//
//            String log = getClient().getLog(action.getId());
//            LOG.info("action log >>>>>>>>>> \n" + log + "\n >>>>>>>>>> \n");
//            LOG.info("update queries >>> " );
//
//            // update query and resume action
//            String newQuery = "SHOW\nTABLES;\n SHOW DATABASES;";
//
//            Map<String, String> updates = new HashMap<String, String>();
//            updates.put("queries", newQuery);
//            getClient().update(action.getId(), updates);
//
//            Thread.sleep(1000);
//            getClient().resume(action.getId());
//
//            Thread.sleep(1000);
//            String status = monitorJob(jobID);
//
//            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
//
//            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
//        } catch (Exception e) {
//            LOG.info("Fail to testHiveUpdateQueriesV31", e);
//            Assert.fail();
//        }
//        LOG.info("    >>>> Pass testHiveUpdateQueriesV31 \n");
//    }
    
//    /**
//     * Test workflow with multiple actions. Test HiveStatus search APIs.
//     *
//     */
//    @Test
//    public void testHiveStatusListV31() {
//
//        try {
//            String dataDir = getClass().getClassLoader().getResource("data/files").getPath();
//            String dataFile = dataDir + "/emp.txt";
//
//            Properties configs = getDefaultProperties();
//
//            // multiple hive actions
//            String appName = "hive-complex";
//            String version = "v31";
//            String appPath = baseAppPath + "/" + version + "/" + appName;
//            configs.put(OozieClient.APP_PATH, appPath);
//            configs.put("appName", appName);
//            configs.put("data_file", dataFile);
//            configs.put("version", version);
//
//            uploadApps(appPath, appName, version);
//
//            //FIXME : validate
//            //getClient().validate(configs);
//
//            String jobID = run(configs);
//
//            WorkflowJob hiveJob = null;
//            WorkflowAction hive1 = null;
//            WorkflowAction hive2 = null;
//            WorkflowAction hiveFinal = null;
//            HiveStatus status1 = null;
//            HiveStatus status2 = null;
//            try {
//
//                for (int i = 0; i < 10; i++) {
//                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
//                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
//                    hiveJob = wfJob;
//                    List<WorkflowAction> actionList = wfJob.getActions();
//                    for (WorkflowAction action : actionList) {
//                        LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
//                        if (action.getName().equals("hive1")) {
//                            hive1 = action;
//                        }
//                        if (action.getName().equals("hive2")) {
//                            hive2 = action;
//                        }
//                        if (action.getName().equals("hive-final")) {
//                            hiveFinal = action;
//                        }
//                    }
//                    if (wfJob.getStatus().equals(WorkflowJob.Status.SUCCEEDED)
//                            || wfJob.getStatus().equals(WorkflowJob.Status.KILLED)
//                            || wfJob.getStatus().equals(WorkflowJob.Status.FAILED)) {
//                        break;
//                    }
//                    Thread.sleep(POLLING);
//                }
//
//
//
//            } catch (Exception e) {
//                LOG.debug("Fail to monitor : " + jobID, e);
//            }
//
//            String status = monitorJob(jobID);
//            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
//
//            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
//
//            List<HiveStatus> hiveStatusList1 = getClient().getHiveStatusListForActionID(hive1.getId());
//            Assert.assertEquals(1, hiveStatusList1.size());
//            for (HiveStatus hiveStatus : hiveStatusList1) {
//                LOG.info("---- hive1 : " + hiveStatus.getJobId() + " : " + hiveStatus.getStatus());
//                Assert.assertTrue(hiveStatus.getJobId().startsWith("job_"));
//                status1 = hiveStatus;
//            }
//
//            List<HiveStatus> hiveStatusList2 = getClient().getHiveStatusListForActionID(hive2.getId());
//            Assert.assertEquals(1, hiveStatusList2.size());
//            for (HiveStatus hiveStatus : hiveStatusList2) {
//                LOG.info("---- hive2 : " + hiveStatus.getJobId() + " : " + hiveStatus.getStatus());
//                Assert.assertTrue(hiveStatus.getJobId().startsWith("job_"));
//                status2 = hiveStatus;
//            }
//
//            List<String> succeededList = new ArrayList<String>();
//            List<HiveStatus> hiveStatusList = getClient().getHiveStatusListForWorkflowID(jobID);
//            // Assert.assertEquals(2, hiveStatusList.size());
//            for (HiveStatus hiveStatus : hiveStatusList) {
//                LOG.info("---- wf : " + hiveStatus.getJobId() + " : " + hiveStatus.getStatus());
//                if (hiveStatus.getStatus().equals("SUCCEEDED")) {
//                    succeededList.add(hiveStatus.getJobId());
//                }
//            }
//
//            Assert.assertTrue(succeededList.contains(status1.getJobId()));
//            Assert.assertTrue(succeededList.contains(status2.getJobId()));
//
//        } catch (Exception e) {
//            LOG.info("Fail to testHiveMRV31", e);
//            Assert.fail();
//        }
//        LOG.info("    >>>> Pass testHiveStatusList \n");
//    }
    
    /**
     * Test workflow with multiple actions.
     * 
     */
    @Test
    public void testSuspendResumeActionV31() {
        
        try {
            String dataDir = getClass().getClassLoader().getResource("data/files").getPath();
            String dataFile = dataDir + "/emp.txt";
            
            Properties configs = getDefaultProperties();
            
            // multiple hive actions
            String appName = "hive-complex";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("data_file", dataFile);
            configs.put("version", version);
            
            uploadApps(appPath, appName, version);
            
            String jobID = run(configs);
            
            WorkflowAction hiveFinal = null;
            for (int i = 0; i < 50; i++) {
                WorkflowJob wfJob = getClient().getJobInfo(jobID);
                List<WorkflowAction> actionList = wfJob.getActions();
                for (WorkflowAction action : actionList) {
                    if (action.getName().equals("hive-final")) {
                        hiveFinal = action;
                    }
                }
                if (hiveFinal != null) {
                    break;
                }
                Thread.sleep(POLLING);
            }
            
            Assert.assertNotNull(hiveFinal);
            
            // suspend action
            LOG.info("suspend hive-Final");
            getClient().suspend(hiveFinal.getId());
            Thread.sleep(3000);
            WorkflowJob wfJob = getClient().getJobInfo(jobID);
            List<WorkflowAction> actionList = wfJob.getActions();
            for (WorkflowAction action : actionList) {
                if (action.getName().equals("hive-final")) {
                    hiveFinal = action;
                }
            }
            
            LOG.info("hive-Final : " + hiveFinal.getStatus().toString());
            Assert.assertEquals(WorkflowAction.Status.START_MANUAL, hiveFinal.getStatus());
            
            // resume action
            LOG.info("resume hive-Final");
            getClient().resume(hiveFinal.getId());
            Thread.sleep(1000);
            
            String status = monitorJob(jobID);
            
            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
            
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
        } catch (Exception e) {
            LOG.info("Fail to testSuspendResumeAction", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testSuspendResumeAction \n");
        
    }
    
    /**
     * Test external transition dependency.
     * 
     */
    @Test
    public void testExternalTransitionDependencyV31() {
        
        try {
            Properties configs = getDefaultProperties();
            
            // ehco external-transition-dependency
            String appName = "shell-dependency";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            
            uploadApps(appPath, appName, version);
            
            String jobID = run(configs);
            
            WorkflowAction dependencyAction = null;
            try {
                for (int i = 0; i < 5; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                        if (action.getName().equals("shell-dependency")) {
                            dependencyAction = action;
                        }
                    }
                    Thread.sleep(POLLING);
                }
            } catch (Exception e) {
                LOG.debug("Fail to monitor : " + jobID, e);
            }
            Assert.assertEquals(WorkflowAction.Status.PREP.toString(), dependencyAction.getStatus().toString());
            LOG.info("......");
            LOG.info("......\n");
            
            // execute dependent workflow
            Properties configs1 = getDefaultProperties();
            String appName1 = "shell";
            String appPath1 = baseAppPath + "/" + version + "/" + appName;
            configs1.put(OozieClient.APP_PATH, appPath1);
            configs1.put("appName", appName1);
            configs1.put("version", version);

            configs1.put("lama.application.id", "-2");
            
            uploadApps(appPath1, appName1, version);
            String jobID1 = run(configs1);
            String status1 = monitorJob(jobID1);
            LOG.info("DONE WORKFLOW1 >> " + jobID1 + " [" + status1 + "]\n");
            
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status1);
            
            Thread.sleep(5000);
            
            // check the external transition dependency
            
            String status = monitorJob(jobID);
            
            LOG.info("DONE External Dependency JOB >> " + jobID + " [" + status + "]");
            
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
            
        } catch (Exception e) {
            LOG.info("Fail to testExternalTransitionDependency", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testExternalTransitionDependency \n");
    }
    
    /**
     * Test decision action .
     * 
     */
    @Test
    public void testDecisionV31() {
        try {
            Properties configs = getDefaultProperties();
            
            String abc = "15";
            
            // if abc is greater than 10 and execute gt.
            String appName = "decision";
            String version = "v31";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            configs.put("abc", abc);
            
            uploadApps(appPath, appName, version);
            
            String jobID = run(configs);
            
            String status = "";
            WorkflowAction gt = null;
            try {
                for (int i = 0; i < 10; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.debug(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        LOG.debug("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                        if (action.getName().equals("gt")) {
                            gt = action;
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
            
            Assert.assertNotNull(gt);
            Assert.assertEquals(gt.getStatus().toString(), WorkflowAction.Status.OK.toString());
            
            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");
            
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);
        } catch (Exception e) {
            LOG.info("Fail to testDecisionV31", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testDecisionV31 \n");
    }
    

}
