package org.apache.oozie.action.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.QueryPlan;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.util.StringUtils;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.wf.ActionCheckXCommand;
import org.apache.oozie.executor.jpa.HiveStatusInsertJPAExecutor;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;


import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.oozie.action.ActionExecutorException.ErrorType.FAILED;


// created per action
public class HiveSession {

    private final XLog LOG = XLog.getLog(HiveSession.class);

    final String wfID;
    final String actionID;
    final String actionName;

    ThriftHive.Client client;
    int maxFetch;

    Configuration configuration;
    String user;
    String group;

    String[] queries;
    Map<String, Map<String, HiveQueryStatusBean>> status; // queryID#stageID --> StatusBean

    boolean killed;
    boolean cleaned;

    transient int index;
    transient Executor executor;

    transient JobClient jobClient;

    JPAService jpaService;

    public HiveSession(String wfID, String actionName, ThriftHive.Client client, String[] queries, int maxFetch) {
        this.wfID = wfID;
        this.actionID = Services.get().get(UUIDService.class).generateChildId(wfID, actionName);
        this.actionName = actionName;
        this.client = client;
        this.queries = queries;
        this.maxFetch = maxFetch;
        this.jpaService = Services.get().get(JPAService.class);
        this.status = new LinkedHashMap<String, Map<String, HiveQueryStatusBean>>();
    }

    public void setUGI(Configuration configuration, String user, String group) {
        this.configuration = configuration;
        this.user = user;
        this.group = group;
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

    private synchronized boolean isFinal() {
        return killed || index >= queries.length;
    }

    private synchronized boolean isCompleted() {
        return isFinal() && (executor == null || executor.executed || executor.ex != null);
    }

    private synchronized boolean executeNext(Context context, WorkflowAction action) {
        if (!isFinal()) {
            executor = new Executor(context, action, queries[index++], index);
            Services.get().get(CallableQueueService.class).queue(executor);
            return true;
        }
        return false;
    }

    public synchronized void execute(Context context, WorkflowAction action) {
        if (!executeNext(context, action)) {
            cleanup(context);
        }
    }

    public synchronized void check(Context context) throws Exception {
        if (executor != null && executor.ex != null) {
            cleanup(context);
            throw new ActionExecutorException(FAILED,
                    "HIVE-002", "failed to execute query {0}", executor.toString(), executor.ex);
        }
        if (isCompleted()) {
            cleanup(context);
        }
    }

    // called by CallbackServlet --> CompletedActionXCommand
    public synchronized void callback(String queryID, String stageID, String jobID, String jobStatus) {
        if (executor != null && executor.inverted != null && jobStatus.equals("SUCCEEDED")) {
            executor.finishPrev(queryID, stageID);
        }
        HiveQueryStatusBean status = updateStatus(queryID, stageID, jobID, jobStatus);
        if (jobStatus.equals("STARTED")) {
            try {
                RunningJob job = jobClient().getJob(JobID.forName(jobID));
                if (job != null) {
                    Services.get().get(CallableQueueService.class).queue(new Polling(status, job));
                }
            } catch (Exception e) {
                LOG.info("Failed to start polling job " + jobID, e);
            }
        }
    }

    public synchronized boolean kill() {
        killed = true;
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
        return executor != null;
    }

    public synchronized void shutdown() {
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.debug("Failed to shutdown hive connection", e);
        }
        if (jobClient != null) {
            try {
                jobClient.close();
            } catch (Exception e) {
                LOG.debug("Failed to shutdown job client", e);
            }
        }
    }

    private void killJob(HiveQueryStatusBean stage) throws Exception {
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

    private void cleanup(Context context) {
        if (cleaned) {
            return;
        }
        LOG.info("Cleaning up hive session");
        context.setExecutionData("OK", null);   // induce ActionEndXCommand
        if (killed) {
            kill();
        }
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.info("Failed to shutdown hive connection", e);
        }
        cleaned = true;
    }

    private synchronized HiveQueryStatusBean updateStatus(String queryID, String stageId, String jobID, String jobStatus) {
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
        updateStatus(stage);
        return stage;
    }

    private void updateStatus(HiveQueryStatusBean stage) {
        try {
            jpaService.execute(new HiveStatusInsertJPAExecutor(stage));
        } catch (Exception e) {
            LOG.info("Failed to update hive status", e);
        }
    }

    private JobClient jobClient() throws HadoopAccessorException {
        if (jobClient == null) {
            JobConf jobConf = new JobConf();
            XConfiguration.copy(configuration, jobConf);
            jobClient = Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
        }
        return jobClient;
    }

    private class Executor implements Runnable {

        Context context;
        WorkflowAction action;
        String query;
        int current;

        String queryID;
        Map<String, Set<String>> inverted;

        volatile Exception ex;
        volatile boolean executed;

        public Executor(Context context, WorkflowAction action, String query, int current) {
            this.context = context;
            this.action = action;
            this.query = query;
            this.current = current;
        }

        @Override
        public void run() {
            LOG.info("Executing query " + this);
            try {
                executeSQL();
                executeNext(context, action);
            } catch (Exception e) {
                ex = e;
                LOG.warn("Failed to execute query {0}, by exception {1}", this, e.toString());
            } finally {
                LOG.info("Executed " + this + " with " + resultCode());
                if (Thread.currentThread().isInterrupted()) {
                    LOG.debug("Thread was interrupted");
                }
            }
            if (checkAction()) {
                CallableQueueService service = Services.get().get(CallableQueueService.class);
                service.queue(new ActionCheckXCommand(actionID));
            }
        }

        private boolean executeSQL() throws Exception {

            QueryPlan plan = client.compile(query);
            if (plan.getQueriesSize() == 0) {
                LOG.info("Query " + this + " is not SQL command");
                client.clean();
                return false;
            }
            Query query = plan.getQueries().get(0);
            LOG.debug(query.getStageGraph());

            queryID = query.getQueryId();
            inverted = invertMapping(query);

            boolean containsMR = false;
            List<Stage> stages = query.getStageList();
            if (stages != null && !stages.isEmpty()) {
                for (Stage stage : stages) {
                    StageType stageType = stage.getStageType();
                    boolean mapreduce = stageType == StageType.MAPRED || stageType == StageType.MAPREDLOCAL;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(stage.toString());
                    }
                    LOG.info("Preparing for query " + query.getQueryId() + " in stage " + stage.getStageId() + " type " + stageType);
                    updateStatus(query.getQueryId(), stage.getStageId(), "NOT_ASSIGNED", mapreduce ? "NOT_STARTED" : stageType.name());
                    containsMR |= mapreduce;
                }
                if (containsMR) {
                    String callback = context.getCallbackUrl("$jobStatus") +
                            "jobId=$jobId&stageId=$stageId&queryId=" + query.getQueryId();
                    LOG.debug("Oozie callback handler = " + callback);
                    client.executeTransient("set hiveconf:task.notification.url=" + callback);
                }
            }
            client.run();

            for (String result : client.fetchN(maxFetch)) {
                LOG.info(result);
            }
            client.clean();

            if (stages != null && !stages.isEmpty()) {
                for (Stage stage : stages) {
                    updateStatus(query.getQueryId(), stage.getStageId(), null, "SUCCEEDED");
                }
            }
            executed = true;

            return containsMR;
        }

        private String resultCode() {
            return ex == null ? "SUCCEEDED" : "FAILED";
        }

        private Map<String, Set<String>> invertMapping(Query query) {
            Map<String, Set<String>> inverted = new HashMap<String, Set<String>>();
            List<Adjacency> adjacencyList = query.getStageGraph().getAdjacencyList();
            if (adjacencyList != null && !adjacencyList.isEmpty()) {
                for (Adjacency adjacency : adjacencyList) {
                    for (String child : adjacency.getChildren()) {
                        Set<String> set = inverted.get(adjacency.getNode());
                        if (set == null) {
                            inverted.put(adjacency.getNode(), set = new HashSet<String>());
                        }
                        set.add(child);
                    }
                }
            }
            LOG.debug("Inverted mapping " + inverted);
            return inverted;
        }

        private void finishPrev(String queryID, String stageID) {
            Set<String> previous = inverted.get(stageID);
            if (previous != null && !previous.isEmpty()) {
                for (String prevID : previous) {
                    finishPrev(queryID, prevID);
                    Map<String, HiveQueryStatusBean> statuses = status.get(queryID);
                    if (statuses == null) {
                        LOG.info("Not registered queryID " + queryID);
                        continue;
                    }
                    HiveQueryStatusBean bean = statuses.get(stageID);
                    if (bean == null || !bean.getStatus().equals("SUCCEEDED")) {
                        updateStatus(queryID, prevID, null, "SUCCEEDED");
                    }
                }
            }
        }

        private boolean checkAction() {
            return ex != null || current >= queries.length;
        }

        @Override
        public String toString() {
            return current + "/" + queries.length;
        }
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
            float[] progress = new float[2];
            try {
                while (!job.isComplete()) {
                    Thread.sleep(1000);

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
                    eventCounter += events.length;
                    for(TaskCompletionEvent event : events) {
                        TaskCompletionEvent.Status status = event.getTaskStatus();
                        if (status == TaskCompletionEvent.Status.FAILED) {
                            this.status.appendFailedTask(event.getTaskAttemptId() + "=" + event.getTaskTrackerHttp());
                            update = true;
                        }
                    }
                    if (update) {
                        updateStatus(status);
                    }
                }
            } catch (Exception e) {
                LOG.info("Polling thread is exiting by exception " + e + ".. retrying in 30sec", e);
                String unique = status.getJobId() + ":" + status.getStageId();
                Services.get().get(CallableQueueService.class).queue(this, "mr.polling", unique, 30000l);
            }
        }
    }
}
