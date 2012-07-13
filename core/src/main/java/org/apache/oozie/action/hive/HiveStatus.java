package org.apache.oozie.action.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.hadoop.JavaActionExecutor;
import org.apache.oozie.executor.jpa.HiveStatusInsertJPAExecutor;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// for rhive action
public class HiveStatus {

    final XLog LOG = XLog.getLog(HiveStatus.class);

    final String wfID;
    final String actionID;
    final String actionName;

    final boolean monitoring;

    Configuration configuration;
    String user;
    String group;
    JPAService jpaService;

    Map<String, Map<String, HiveQueryStatusBean>> status; // queryID#stageID --> StatusBean

    JobClient jobClient;

    public HiveStatus(String wfID, String actionName, boolean monitoring) {
        this.wfID = wfID;
        this.actionID = Services.get().get(UUIDService.class).generateChildId(wfID, actionName);
        this.actionName = actionName;
        this.monitoring = monitoring;
        this.jpaService = Services.get().get(JPAService.class);
        this.status = new LinkedHashMap<String, Map<String, HiveQueryStatusBean>>();
    }

    public void initialize(ActionExecutor.Context context) throws Exception {
        this.configuration = JavaActionExecutor.createBaseHadoopConf(context, context.getActionXML());
        this.user = context.getWorkflow().getUser();
        this.group = context.getWorkflow().getGroup();
    }

    public boolean isInitialized() {
        return configuration != null;
    }

    public List<HiveQueryStatusBean> getStatus() {
        List<HiveQueryStatusBean> result = new ArrayList<HiveQueryStatusBean>();
        for (Map<String, HiveQueryStatusBean> stages : status.values()) {
            for (HiveQueryStatusBean status : stages.values()) {
                result.add(status.duplicate());
            }
        }
        return result;
    }

    public List<HiveQueryStatusBean> getStatus(String queryID) {
        Map<String, HiveQueryStatusBean> stages = status.get(queryID);
        if (stages != null) {
            List<HiveQueryStatusBean> result = new ArrayList<HiveQueryStatusBean>();
            for (HiveQueryStatusBean status : stages.values()) {
                result.add(status.duplicate());
            }
            return result;
        }
        return null;
    }

    public HiveQueryStatusBean getStatus(String queryID, String stageID) {
        Map<String, HiveQueryStatusBean> stages = status.get(queryID);
        if (stages != null) {
            return stages.get(stageID);
        }
        return null;
    }

    // called by CallbackServlet --> CompletedActionXCommand
    public synchronized void callback(String queryID, String stageID, String jobID, String jobStatus) {
        HiveQueryStatusBean status = updateStatus(queryID, stageID, jobID, jobStatus);
        if (monitoring && jobStatus.equals("STARTED") && isInitialized()) {
            try {
                RunningJob job = jobClient().getJob(JobID.forName(jobID));
                if (job == null) {
                    LOG.info("Cannot access running job " + jobID + " for monitoring");
                } else {
                    Services.get().get(CallableQueueService.class).queue(new Polling(status, job));
                }
            } catch (Exception e) {
                LOG.info("Failed to start polling job " + jobID, e);
            }
        }
    }

    public boolean shutdown() {
        killJobs();
        killJobClient();
        return true;
    }

    private synchronized void killJobs() {
        for (Map<String, HiveQueryStatusBean> stages : status.values()) {
            for (HiveQueryStatusBean stage : stages.values()) {
                if (stage.getStatus().equals("STARTED")) {
                    try {
                        killJob(stage);
                    } catch (Throwable e) {
                        LOG.warn("Failed to kill stage " + stage.toString(), e);
                    }
                }
            }
        }
        status.clear();
    }

    private synchronized void killJob(HiveQueryStatusBean stage) throws Exception {
        if (!isInitialized()) {
            return;
        }
        RunningJob runningJob = jobClient().getJob(JobID.forName(stage.getJobId()));
        if (runningJob != null && !runningJob.isComplete()) {
            LOG.info("Killing MapReduce job " + runningJob.getID());
            try {
                runningJob.killJob();
            } catch (Exception e) {
                LOG.info("Failed to kill MapReduce job " + runningJob.getID(), e);
            }
        }
    }

    private synchronized void killJobClient() {
        if (jobClient != null) {
            try {
                jobClient.close();
            } catch (Exception e) {
                LOG.debug("Failed to shutdown job client", e);
            }
        }
    }

    private synchronized JobClient jobClient() throws HadoopAccessorException {
        if (jobClient == null) {
            JobConf jobConf = new JobConf();
            XConfiguration.copy(configuration, jobConf);
            jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
        }
        return jobClient;
    }

    HiveQueryStatusBean updateStatus(String queryID, String stageId, String jobID, String jobStatus) {
        return updateStatus(updateBean(queryID, stageId, jobID, jobStatus));
    }

    HiveQueryStatusBean updateStatus(HiveQueryStatusBean stage) {
        try {
            jpaService.execute(new HiveStatusInsertJPAExecutor(stage));
            return stage;
        } catch (Exception e) {
            LOG.info("Failed to update hive status", e);
        }
        return null;
    }

    private synchronized HiveQueryStatusBean updateBean(String queryID, String stageId, String jobID, String jobStatus) {
        HiveQueryStatusBean stage = null;
        Map<String, HiveQueryStatusBean> stages = status.get(queryID);
        if (stages == null) {
            status.put(queryID, stages = new LinkedHashMap<String, HiveQueryStatusBean>());
        } else {
            stage = stages.get(stageId);
        }
        if (stage == null) {
            stage = new HiveQueryStatusBean();
            stage.setStartTime(new Date());
            stage.setWfId(wfID);
            stage.setActionId(actionID);
            stage.setActionName(actionName);
            stages.put(stageId, stage);
        }
        stage.setQueryId(queryID);
        stage.setStageId(stageId);
        if (jobID != null) {
            stage.setJobId(jobID);
        }
        stage.setStatus(jobStatus);
        if (!jobStatus.equals("NOT_STARTED")) {
            stage.setEndTime(new Date());
        }
        return stage;
    }

    // from JobClient#monitorAndPrintJob
    private class Polling implements Runnable {

        HiveQueryStatusBean status;
        RunningJob job;

        Polling(HiveQueryStatusBean status, RunningJob job) {
            this.status = status;
            this.job = job;
        }

        public void run() {
            int eventCounter = 0;
            float[] progress = new float[] {-1, -1};
            try {
                while (!job.isComplete()) {
                    eventCounter += monitor(eventCounter, progress);
                    Thread.sleep(1000);
                }
                monitor(eventCounter, progress);
            } catch (Exception e) {
                LOG.info("Polling thread is exiting by exception " + e + ".. retrying in 30sec", e);
                String unique = status.getJobId() + ":" + status.getStageId();
                Services.get().get(CallableQueueService.class).queue(this, "mr.polling", unique, 30000l);
            }
        }


        private int monitor(int eventCounter, float[] progress) throws IOException {
            if (job.mapProgress() != progress[0] || job.reduceProgress() != progress[1]) {
                StringBuilder builder = new StringBuilder();
                builder.append(" map ").append(StringUtils.formatPercent(job.mapProgress(), 0));
                builder.append(" reduce ").append(StringUtils.formatPercent(job.reduceProgress(), 0));
                LOG.info(builder.toString());
                progress[0] = job.mapProgress();
                progress[1] = job.reduceProgress();
            }

            boolean update = false;
            TaskCompletionEvent[] events = job.getTaskCompletionEvents(eventCounter);
            for(TaskCompletionEvent event : events) {
                TaskCompletionEvent.Status taskStatus = event.getTaskStatus();
                if (taskStatus == TaskCompletionEvent.Status.FAILED) {
                    status.appendFailedTask(event.getTaskAttemptId() + "=" + event.getTaskTrackerHttp());
                    update |= true;
                }
            }
            if (update) {
                updateStatus(status);
            }
            return events.length;
        }
    }

    public static String getTaskLogURL(String taskId, String baseUrl) {
        return baseUrl + "/tasklog?attemptid=" + taskId + "&all=true";
    }
}
