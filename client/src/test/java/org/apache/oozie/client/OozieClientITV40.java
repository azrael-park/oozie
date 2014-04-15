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
import org.apache.oozie.client.rest.RestConstants;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;

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
            WorkflowAction shellAction = null;
            String status = "";
            String capture = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.info(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if(action.getName().equals("shell-1")){
                            LOG.info("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                            LOG.info("    " + "capture >> \n " + action.getData() + "\n");
                            capture = action.getData();
                            shellAction = action;
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
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);

            Assert.assertTrue(capture.contains("hello-standard-output"));

            Assert.assertNotNull(shellAction);
            LOG.info(" ---- JOB LOG ----");
            LOG.info(getClient().getJobLog(jobID));
            LOG.info(" ---- JOB LOG end ----\n");
//            LOG.info(" ---- Action LOG ----");
//            LOG.info(getClient().getLog(shellAction.getId()));
//            LOG.info(" ---- Action LOG end ----");
        } catch (Exception e) {
            LOG.info("Fail to testShellOutStreamV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testShellOutStreamV40 \n");
    }

    /**
     * Test kill node with error during setting error message.
     *
     */
    @Test
    public void testKillNodeErrorV40() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "kill-error";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);
            String status = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.info(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        LOG.info("    " + action.getName() + " [" + action.getStatus().toString() + "]");
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
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.FAILED.toString(), status);

        } catch (Exception e) {
            LOG.info("Fail to testKillNodeErrorV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testKillNodeErrorV40 \n");
    }

    /**
     * Test kill node with error during setting error message.
     *
     */
    @Test
    public void testKillNodeError3V40() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "kill-error-3";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);
            String status = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.info(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        LOG.info("    " + action.getName() + " [" + action.getStatus().toString() + "]");
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
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.FAILED.toString(), status);

        } catch (Exception e) {
            LOG.info("Fail to testKillNodeError3V40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testKillNodeError3V40 \n");
    }



    /**
     * Test capture std-out, std-err from shell action .
     *
     */
    @Test
    public void testHiveOrgV40() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "hive-oozie-org";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version, "hive-query.q");

            String jobID = run(configs);
            WorkflowAction hiveAction = null;
            String status = "";
            String capture = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.info(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if(action.getName().equals("hive-init")){
                            LOG.info("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                            LOG.info("    " + "capture >> \n " + action.getData() + "\n");
                            capture = action.getData();
                            hiveAction = action;
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
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);

            Assert.assertTrue(capture.contains("hello-standard-output"));

            Assert.assertNotNull(hiveAction);
            LOG.info(" ---- JOB LOG ----");
            LOG.info(getClient().getJobLog(jobID));
            LOG.info(" ---- JOB LOG end ----\n");
//            LOG.info(" ---- Action LOG ----");
//            LOG.info(getClient().getLog(shellAction.getId()));
//            LOG.info(" ---- Action LOG end ----");
        } catch (Exception e) {
            LOG.info("Fail to testHiveOrgV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testHiveOrgV40 \n");
    }

    /**
     * Test hive2 action.
     *
     */
    @Test
    public void testHive2OrgV40() {
        try {
            Properties configs = getDefaultProperties();

            // ehco
            String appName = "hive2-oozie-org";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            configs.put("oozie.use.system.libpath", "true");

            uploadApps(appPath, appName, version, "hive-query.q");

            String jobID = run(configs);
            WorkflowAction hiveAction = null;
            String status = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.info(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if(action.getName().equals("hive-init")){
                            LOG.info("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                            LOG.info("    " + "capture >> \n " + action.getData() + "\n");
                            hiveAction = action;
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
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);

            Assert.assertNotNull(hiveAction);
            LOG.info(" ---- JOB LOG ----");
            LOG.info(getClient().getJobLog(jobID));
            LOG.info(" ---- JOB LOG end ----\n");
        } catch (Exception e) {
            LOG.info("Fail to testHiveOrgV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testHiveOrgV40 \n");
    }

    /**
     * Test hive2 action.
     *
     */
    @Test
    public void testHive2V40() {
        try {
            // ehco
            String appName = "hive2";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;

            String srcDir = definitionDir + "/" + version + "/" + appName;
            Properties configs = getProperties(srcDir + "/job.properties");

            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("version", version);
            configs.put("appName", appName);

            deployApp(srcDir, appPath);

            String jobID = run(configs);
            WorkflowAction hiveAction = null;
            String status = "";
            try {
                for (int i = 0; i < 50; i++) {
                    WorkflowJob wfJob = getClient().getJobInfo(jobID);
                    LOG.info(wfJob.getId() + " [" + wfJob.getStatus().toString() + "]");
                    List<WorkflowAction> actionList = wfJob.getActions();
                    for (WorkflowAction action : actionList) {
                        if(action.getName().equals("hive-init")){
                            LOG.info("    " + action.getName() + " [" + action.getStatus().toString() + "]");
                            LOG.info("    " + "capture >> \n " + action.getData() + "\n");
                            hiveAction = action;
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
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);

            Assert.assertNotNull(hiveAction);
            LOG.info(" ---- JOB LOG ----");
            LOG.info(getClient().getJobLog(jobID));
            LOG.info(" ---- JOB LOG end ----\n");
        } catch (Exception e) {
            LOG.info("Fail to testHiveOrgV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testHiveOrgV40 \n");
    }

    /**
     * Test very simple FS action .
     *
     */
    @Test
    public void testFSV40() {
        try {
            Properties configs = getDefaultProperties();

            String appName = "fs";
            String version = "v40";
            String appPath = baseAppPath + "/" + appName;
            configs.put(OozieClient.APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);

            uploadApps(appPath, appName, version);

            String jobID = run(configs);
            String status = monitorJob(jobID);

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), status);

            WorkflowAction hdfsAction = null;
            WorkflowJob wfJob = getClient().getJobInfo(jobID);
            List<WorkflowAction> actionList = wfJob.getActions();
            for (WorkflowAction action : actionList) {
                if (action.getName().equals("hdfscommands")) {
                    hdfsAction = action;
                }
            }
            Assert.assertNotNull(hdfsAction);
            LOG.info(" ---- JOB LOG ----");
            LOG.info(getClient().getJobLog(jobID));
            LOG.info(" ---- JOB LOG end ----");

//            LOG.info(" ---- Action LOG ----");
//            LOG.info(getClient().getLog(hdfsAction.getId()));
//            LOG.info(" ---- Action LOG end ----");


        } catch (Exception e) {
            LOG.info("Fail to testFSV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testFSV40 \n");
    }

    @Test
    public void testWFFilter() {
        try {
            StringBuffer filter = new StringBuffer();
            filter.append(OozieClient.FILTER_USER + "=" + "test" + ";");
//            filter.append(OozieClient.FILTER_NAME + "=" + "" + ";");
//            filter.append(OozieClient.FILTER_GROUP + "=" + "" + ";");
//            filter.append(OozieClient.FILTER_STATUS + "=" + "" + ";");
            filter.append(OozieClient.FILTER_ID + "=" + "0000000-140508134117180-oozie-seoe-W");

            List<WorkflowJob> jobs = getClient().getJobsInfo(filter.toString());

            LOG.info("-- size  : " + jobs.size());
            for (WorkflowJob job: jobs) {
                Assert.assertEquals(job.getId(), "0000000-140508134117180-oozie-seoe-W");
            }
        } catch (Exception e) {
            LOG.info("Fail to testWFFilter", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testWFFilter \n");
    }

    /**
     * Test simple coordinator minutely.
     *
     */
    @Test
    public void testCoordSimpleV40() {
        try {
            Properties configs = getDefaultProperties();

            //Calendar current = Calendar.getInstance(TimeZone.getTimeZone("Asia/Seoul"));
            Calendar current = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            String startTime = printDateTimeInISO8601(current);
            Calendar end =  (Calendar)current.clone();
            end.add(Calendar.MINUTE, 2);
            String endTime = printDateTimeInISO8601(end);
            //startTime = "2014-07-11T01:00Z";
            //endTime = "2014-07-11T01:02Z";
            LOG.info("---- startTime : " + startTime);
            LOG.info("---- endTime : " + endTime);
            LOG.info("---- timeZone : " + TimeZone.getDefault());

            // ehco
            String appName = "coord-simple";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.COORDINATOR_APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            configs.put("startTime", startTime);
            configs.put("endTime", endTime);

            uploadCoordApps(appPath, appName, version);

            String jobID = run(configs);

            String status = "";
            try {
                for (int i = 0; i < 50; i++) {
                    CoordinatorJob coordJob = getClient().getCoordJobInfo(jobID);
                    List<CoordinatorAction> actions = coordJob.getActions();
                    LOG.info(coordJob.getId() + " [" + coordJob.getStatus().toString() + "]");
                    for (CoordinatorAction action : actions) {
                        LOG.info("    " + action.getId() + " [" + action.getStatus().toString() + "]");
                    }
                    status = coordJob.getStatus().toString();
                    if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                            || coordJob.getStatus() == CoordinatorJob.Status.KILLED
                            || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
                        break;
                    }
                    Thread.sleep(10000);
                }
            } catch (Exception e) {
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(CoordinatorJob.Status.SUCCEEDED.toString(), status);

            // LOG contains @1, @2
            LOG.info(" ---- JOB LOG start ----");
            String log = getClient().getJobLog(jobID);
            LOG.info(log);
            LOG.info(" ---- JOB LOG end ----\n");

            Assert.assertTrue(log.contains("@1"));
            Assert.assertTrue(log.contains("@2"));
        } catch (Exception e) {
            LOG.info("Fail to testCoordSimpleV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testCoordSimpleV40 \n");
    }


    /**
     * Test simple coordinator minutely.
     *
     */
    @Test
    public void testCoordSimpleDryrunV40() {
        try {
            Properties configs = getDefaultProperties();

            //Calendar current = Calendar.getInstance(TimeZone.getTimeZone("Asia/Seoul"));
            Calendar current = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            current.set(2014, 6, 11, 1, 0, 0);
            String startTime = printDateTimeInISO8601(current);
            Calendar end =  (Calendar)current.clone();
            end.add(Calendar.MINUTE, 2);
            String endTime = printDateTimeInISO8601(end);
            LOG.info("---- startTime : " + startTime);
            LOG.info("---- endTime : " + endTime);
            LOG.info("---- timeZone : " + TimeZone.getDefault());

            // ehco
            String appName = "coord-simple";
            String version = "v40";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.COORDINATOR_APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            configs.put("startTime", startTime);
            configs.put("endTime", endTime);
            configs.put(RestConstants.JOB_ACTION_DRYRUN, true);

            uploadCoordApps(appPath, appName, version);

            String dryRunOutput = getClient().dryrun(configs);
            String[] actions = dryRunOutput.split("action for new instance");
            Assert.assertTrue(actions[0].contains("***actions for instance***"));
            Assert.assertEquals(2, actions.length -1);

        } catch (Exception e) {
            LOG.info("Fail to testCoordSimpleV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testCoordSimpleV40 \n");
    }

    /**
     * Test coordinator using dataSet.
     *
     */
    @Test
    public void testCoordFsV40() {
        try {
            String version = "v40";

            // wf
            String sDate = printDateTime(Calendar.getInstance(), "yyyy/MM/dd/HH");
            LOG.info("---- : " +sDate);
            String wfAppName = "fs-create";
            String wfCreateAppPath = baseAppPath + "/" + version + "/" + wfAppName;
            Properties wfConfigs = getDefaultProperties();
            wfConfigs.put("appName", wfAppName);
            wfConfigs.put("version", version);
            wfConfigs.put(OozieClient.APP_PATH, wfCreateAppPath);
            wfConfigs.put("createPath", "/user/test/"+sDate);
            uploadApps(wfCreateAppPath, wfAppName, version);

            // run wf
            String wfJobId = run(wfConfigs);
            String wfStatus = monitorJob(wfJobId);
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), wfStatus);

            Properties configs = getDefaultProperties();

            Calendar current = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            String startTime = printDateTimeInISO8601(current);
            Calendar end =  (Calendar)current.clone();
            end.add(Calendar.MINUTE, 2);
            String endTime = printDateTimeInISO8601(end);
            LOG.info("---- startTime : " + startTime);
            LOG.info("---- endTime : " + endTime);

            // coord
            String appName = "coord-fs-create";
            String appPath = baseAppPath + "/" + version + "/" + appName;
            configs.put(OozieClient.COORDINATOR_APP_PATH, appPath);
            configs.put("appName", appName);
            configs.put("version", version);
            configs.put("startTime", startTime);
            configs.put("endTime", endTime);
            configs.put("wfCreateAppPath", wfCreateAppPath);

            uploadCoordApps(appPath, appName, version);

            // run coord
            LOG.info("\n ------ start coordinator -----");
            String jobID = run(configs);

            String status = "";
            try {
                for (int i = 0; i < 50; i++) {
                    CoordinatorJob coordJob = getClient().getCoordJobInfo(jobID);
                    List<CoordinatorAction> actions = coordJob.getActions();
                    LOG.info(coordJob.getId() + " [" + coordJob.getStatus().toString() + "]");
                    for (CoordinatorAction action : actions) {
                        LOG.info("    " + action.getId() + " [" + action.getStatus().toString() + "]");
                    }
                    status = coordJob.getStatus().toString();
                    if (coordJob.getStatus() == CoordinatorJob.Status.SUCCEEDED
                            || coordJob.getStatus() == CoordinatorJob.Status.KILLED
                            || coordJob.getStatus() == CoordinatorJob.Status.FAILED) {
                        break;
                    }
                    Thread.sleep(3000);
                }
            } catch (Exception e) {
                LOG.info("Fail to monitor : " + jobID, e);
            }

            LOG.info("DONE JOB >> " + jobID + " [" + status + "]");

            Assert.assertEquals(CoordinatorJob.Status.SUCCEEDED.toString(), status);

            // LOG contains @1, @2
            LOG.info(" ---- JOB LOG ----");
            String log = getClient().getJobLog(jobID);
            LOG.info(log);
            LOG.info(" ---- JOB LOG end ----\n");

            Assert.assertTrue(log.contains("@1"));
            Assert.assertTrue(log.contains("@2"));
        } catch (Exception e) {
            LOG.info("Fail to testCoordSimpleV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testCoordSimpleV40 \n");
    }

    /**
     * Test bundleJob.
     *
     */
    @Test
    public void testBundleFsV40() {
        try {
            Calendar current = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            String startTime = printDateTimeInISO8601(current);
            Calendar end =  (Calendar)current.clone();
            end.add(Calendar.MINUTE, 2);
            String endTime = printDateTimeInISO8601(end);
            LOG.info("---- startTime : " + startTime);
            LOG.info("---- endTime : " + endTime);

            String version = "v40";
            // wf
            String sDate = printDateTime(Calendar.getInstance(), "yyyy/MM/dd/HH");
            LOG.info("---- : " +sDate);
            String wfAppName = "fs-create";
            String wfCreatePath = baseAppPath + "/" + version + "/" + wfAppName;
            Properties wfConfigs = getDefaultProperties();
            wfConfigs.put("appName", wfAppName);
            wfConfigs.put("version", version);
            wfConfigs.put(OozieClient.APP_PATH, wfCreatePath);
            wfConfigs.put("createPath", "/user/test/"+sDate);
            uploadApps(wfCreatePath, wfAppName, version);

            // run wf : input event
            String wfJobId = run(wfConfigs);
            String wfStatus = monitorJob(wfJobId);
            Assert.assertEquals(WorkflowJob.Status.SUCCEEDED.toString(), wfStatus);

            // upload wfdelete
            String wfDeleteName = "fs-delete";
            String wfDeletePath =   baseAppPath + "/" + version + "/" + wfDeleteName;
            uploadApps(wfDeletePath, wfDeleteName, version);

            // upload coords
            String coordCreateName = "coord-fs-create";
            String coordCreateAppPath = baseAppPath + "/" + version + "/" + coordCreateName;
            uploadCoordApps(coordCreateAppPath, coordCreateName, version);
            String coordDeleteName = "coord-fs-delete";
            String coordDeleteAppPath = baseAppPath + "/" + version + "/" + coordDeleteName;
            uploadCoordApps(coordDeleteAppPath, coordDeleteName, version);

            // bundle
            Properties bundleConfigs = getDefaultProperties();
            String bundleAppName = "bundle-fs";
            String bundleAppPath = baseAppPath + "/" + version + "/" + bundleAppName;
            bundleConfigs.put(OozieClient.BUNDLE_APP_PATH, bundleAppPath);
            bundleConfigs.put("appName", bundleAppName);
            bundleConfigs.put("version", version);
            bundleConfigs.put("kickOffTime", startTime);
            bundleConfigs.put("startTime1", startTime);
            bundleConfigs.put("endTime1", endTime);
            bundleConfigs.put("startTime2", startTime);
            bundleConfigs.put("endTime2", endTime);
            bundleConfigs.put("coordCreateAppPath", coordCreateAppPath);
            bundleConfigs.put("coordDeleteAppPath", coordDeleteAppPath);
            bundleConfigs.put("wfCreateAppPath", wfCreatePath);
            bundleConfigs.put("wfDeleteAppPath", wfDeletePath);

            uploadBundleApps(bundleAppPath, bundleAppName, version);

            // run coord
            LOG.info("\n ------ start bundle -----");
            String bundleId = run(bundleConfigs);

            String status = "";
            String bundleFilter = OozieClient.FILTER_ID + "=" + bundleId;
            try {
                for (int i = 0; i < 50; i++) {
                    List<BundleJob> bundleJobs = getClient().getBundleJobsInfo(bundleFilter, 0, 100);

                    for (BundleJob bundleJob : bundleJobs) {
                        LOG.info("    " + bundleJob.getId() + " [" + bundleJob.getStatus().toString() + "]");
                        status = bundleJob.getStatus().toString();
                    }
                    if (status.equals(BundleJob.Status.SUCCEEDED.toString())) {
                        break;
                    }
                    Thread.sleep(3000);
                }
            } catch (Exception e) {
                LOG.info("Fail to monitor : " + bundleId, e);
            }

            LOG.info("DONE JOB >> " + bundleId + " [" + status + "]");

            Assert.assertEquals(BundleJob.Status.SUCCEEDED.toString(), status);

            // LOG contains @1, @2
            LOG.info(" ---- JOB LOG ----");
            String log = getClient().getJobLog(bundleId);
            LOG.info(log);
            LOG.info(" ---- JOB LOG end ----\n");

        } catch (Exception e) {
            LOG.info("Fail to testCoordSimpleV40", e);
            Assert.fail();
        }
        LOG.info("    >>>> Pass testCoordSimpleV40 \n");
    }

    /**
     * Converts a Timestamp value into string representing a date time in UTC using the following date format used in
     * Oozie: yyyy-MM-ddTHH:mmZ. See http://www.w3.org/TR/NOTE-datetime.
     *
     * @param timestamp
     *            A Timestamp value
     * @return A string containing a date-time
     */
    public static String printDateTimeInISO8601(Timestamp timestamp) {
        Calendar uCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        uCalendar.setTimeInMillis(timestamp.getTime());
        String converted = javax.xml.bind.DatatypeConverter.printDateTime(uCalendar);

        int secondColonPos = converted.indexOf(":", converted.indexOf(":") + 1);
        return converted.substring(0, secondColonPos) + "Z";
    }

    /**
     * Converts a Calendar value into string representing a date time in UTC using the following date format used in
     * Oozie: yyyy-MM-ddTHH:mmZ. See http://www.w3.org/TR/NOTE-datetime.
     *
     * @param calendar A Calendar instance created in "GMT" TimeZone.
     * @return A string containing a date-time
     */
    public static String printDateTimeInISO8601(Calendar calendar) {
        String converted = javax.xml.bind.DatatypeConverter.printDateTime(calendar);

        int secondColonPos = converted.indexOf(":", converted.indexOf(":") + 1);
        return converted.substring(0, secondColonPos) + "Z";
    }

    public static String printDateTime(Calendar calendar, String pattern) {
        SimpleDateFormat format = new SimpleDateFormat(pattern);

        String sDate = format.format(calendar.getTime());
        return sDate;
    }

}
