package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.*;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutor.Context;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.wf.ActionCheckXCommand;
import org.apache.oozie.executor.jpa.HiveStatusInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.XLog;
import org.apache.thrift.TException;


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
    int timeout;
    int maxFetch;

    String[] queries;
    Map<String, Map<String, HiveQueryStatusBean>> status; // queryID#stageID --> StatusBean

    boolean killed;
    boolean cleaned;

    transient int index;
    transient Executor executor;

    JPAService jpaService;

    public HiveSession(String wfID, String actionName, ThriftHive.Client client, String[] queries, int timeout, int maxFetch) {
        this.wfID = wfID;
        this.actionID = Services.get().get(UUIDService.class).generateChildId(wfID, actionName);
        this.actionName = actionName;
        this.client = client;
        this.queries = queries;
        this.timeout = timeout;
        this.maxFetch = maxFetch;
        this.jpaService = Services.get().get(JPAService.class);
        this.status = new LinkedHashMap<String, Map<String, HiveQueryStatusBean>>();
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

    public synchronized void callback(String queryID, String stageID, String jobID, String jobStatus) {
        if (executor != null && executor.inverted != null && jobStatus.equals("SUCCEEDED")) {
            executor.finishPrev(queryID, stageID);
        }
        updateStatus(queryID, stageID, jobID, jobStatus);
    }

    public synchronized boolean kill(WorkflowJob workflow) {
        killed = true;
        JobClient client = null;
        try {
            for (Map<String, HiveQueryStatusBean> stages : status.values()) {
                for (HiveQueryStatusBean stage : stages.values()) {
                    if (stage.getStatus().equals("STARTED")) {
                        try {
                            client = killJob(workflow, client, stage);
                        } catch (Throwable e) {
                            LOG.warn("Failed to kill stage " + stage.toString(), e);
                        }
                    }
                }
            }
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {
                    LOG.debug("Failed to close job client", e);
                }
            }
        }
        return executor != null;
    }

    public synchronized void shutdown() {
        try {
            client.shutdown();
        } catch (TException e) {
            LOG.info("Failed to shutdown hive connection", e);
        }
    }

    private JobClient killJob(WorkflowJob workflow, JobClient client, HiveQueryStatusBean stage) throws Exception {
        client = client == null ? createJobClient(workflow, new JobConf()) : client;
        RunningJob runningJob = client.getJob(JobID.forName(stage.getJobId()));
        if (runningJob != null && !runningJob.isComplete()) {
            LOG.info("Killing MapReduce job " + runningJob.getID());
            try {
                runningJob.killJob();
            } catch (Exception e) {
                LOG.info("Failed to kill MapReduce job " + runningJob.getID(), e);
            }
        }
        return client;
    }

    private void cleanup(Context context) {
        if (cleaned) {
            return;
        }
        LOG.info("Cleaning up hive session");
        context.setExecutionData("OK", null);   // induce ActionEndXCommand
        if (killed) {
            kill(context.getWorkflow());
        }
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.info("Failed to shutdown hive connection", e);
        }
        cleaned = true;
    }

    private synchronized void updateStatus(String queryID, String stageId, String jobID, String jobStatus) {
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
        try {
            jpaService.execute(new HiveStatusInsertJPAExecutor(stage));
        } catch (Exception e) {
            LOG.info("Failed to update hive status", e);
        }
    }

    private JobClient createJobClient(WorkflowJob workflow, JobConf jobConf) throws HadoopAccessorException {
        String user = workflow.getUser();
        return Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
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
}
