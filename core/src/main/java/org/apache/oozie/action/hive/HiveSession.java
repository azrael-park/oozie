package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.*;
import org.apache.hadoop.hive.service.HiveServerException;
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
import org.apache.oozie.service.*;
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
import java.util.concurrent.Callable;

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

    transient int index;
    transient Query query;
    transient Map<String, Set<String>> inverted;

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
                result.add(status.clone());
            }
        }
        return result;
    }

    public List<HiveQueryStatusBean> getStatus(String queryID) {
        Map<String, HiveQueryStatusBean> stages = status.get(queryID);
        if (stages != null) {
            List<HiveQueryStatusBean> result = new ArrayList<HiveQueryStatusBean>();
            for (HiveQueryStatusBean status : stages.values()) {
                result.add(status.clone());
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

    public boolean isCompleted() {
        return killed || index == queries.length;
    }

    public synchronized void execute(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.info("executing " + index + "/" + queries.length);
        for (; !killed && index < queries.length; index++) {
            Executor executor = compile(context, action, queries[index]);
            if (executor != null) {
                this.executor = executor;
                Services.get().get(CallableQueueService.class).queue(executor);
                return;
            }
        }
        check(context, action);
    }

    public synchronized void check(Context context, WorkflowAction action) throws ActionExecutorException {
        LOG.debug("check called " + index + "/" + queries.length);
        if (isCompleted()) {
            cleanup(context, action);
        } else {
            if (executor != null && executor.completed) {
                if (executor.ex != null) {
                    throw new ActionExecutorException(FAILED, "HIVE-002", "failed to execute query {0}", queries[index], executor.ex);
                }
                executor = null;
                index++;
                execute(context, action);
            }
        }
    }

    public void callback(String queryID, String stageID, String jobID, String jobStatus) {
        finishPrevs(queryID, stageID);
        updateStatus(queryID, stageID, jobID, jobStatus);
    }

    private void finishPrevs(String queryID, String stageID) {
        Set<String> previous = inverted.get(stageID);
        if (previous != null && !previous.isEmpty()) {
            for (String prevID : previous) {
                finishPrevs(queryID, prevID);
                HiveQueryStatusBean bean = status.get(queryID).get(stageID);
                if (bean == null || !bean.getStatus().equals("SUCCEEDED")) {
                    updateStatus(queryID, prevID, null, "SUCCEEDED");
                }
            }
        }
    }

    public boolean kill(Context context) throws Exception {
        return kill(context.getWorkflow());
    }

    public synchronized boolean kill(WorkflowJob workflow) throws Exception {
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

    private void cleanup(Context context, WorkflowAction action) {
        LOG.debug("Cleaning up called for action " + action.getId());
        context.setExecutionData("OK", null);   // induce ActionEndXCommand
        if (killed) {
            try {
                kill(context);
            } catch (Exception e) {
                // ignore
            }
        }
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.info("Failed to clean hive connection", e);
        }
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
            stage.setPersisted(false);
            stage.setWfId(wfID);
            stage.setActionId(actionID);
            stage.setActionName(actionName);
            stages.put(stageId, stage);
        }
        stage.setQueryId(queryID);
        stage.setStageId(stageId);
        stage.setJobId(jobID);
        stage.setStatus(jobStatus);
        if (!jobStatus.equals("NOT_STARTED")) {
            stage.setEndTime(new Date());
        }
        try {
            jpaService.execute(new HiveStatusInsertJPAExecutor(stage));
        } catch (JPAExecutorException e) {
            LOG.warn("Failed to insert hive status", e);
        }
    }

    private Executor compile(Context context, WorkflowAction action, final String sql) throws ActionExecutorException {

        LOG.debug("Compiling SQL " + sql);

        try {
            QueryPlan plan = execute(new Callable<QueryPlan>() {
                public QueryPlan call() throws Exception { return client.compile(sql); }
            }, timeout);
            if (plan.getQueriesSize() == 0) {
                LOG.debug(sql + " is non-hive query");
                return null;
            }
            Query query = plan.getQueries().get(0);

            prepareQuery(query);

            Executor executor = new Executor(action.getId());
            List<Stage> stages = query.getStageList();
            if (stages != null && !stages.isEmpty()) {
                boolean containsMR = false;
                for (Stage stage : stages) {
                    boolean mapreduce = stage.getStageType() == StageType.MAPRED
                            || stage.getStageType() == StageType.MAPREDLOCAL;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(stage.toString());
                    }
                    updateStatus(query.getQueryId(), stage.getStageId(), null, mapreduce ? "NOT_STARTED" : "NONE_MR");
                    containsMR |= mapreduce;
                }
                if (containsMR) {
                    String callback = context.getCallbackUrl("$jobStatus") + "jobId=$jobId&stageId=$stageId&queryId=" + query.getQueryId();
                    LOG.debug("-- callback = " + callback);
                    client.executeTransient("set hiveconf:task.notification.url=" + callback);
                    return executor;
                }
                // excute directly for EXPLAIN or simple DDL tasks etc.
            }
            LOG.debug(sql + " is non-MR query");
            executor.execute();

            if (stages != null && !stages.isEmpty()) {
                for (Stage stage : stages) {
                    updateStatus(query.getQueryId(), stage.getStageId(), null, "SUCCESS");
                }
            }
        } catch (Throwable e) {
            throw new ActionExecutorException(FAILED, "HIVE-002", "failed to execute query {0}", sql, e);
        }
        return null;
    }

    private void prepareQuery(Query query) {
        LOG.info(query.getStageGraph());

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
        this.query = query;
        this.inverted = inverted;
    }

    private <T> T execute(final Callable<T> callable, int timeout) throws Exception {
        if (timeout < 0) {
            return callable.call();
        }
        final Wait<T> wait = new Wait<T>();
        Services.get().get(CallableQueueService.class).queue(new Runnable() {
            public void run() {
                if (!wait.runner(Thread.currentThread())) {
                    try {
                        wait.result(callable.call());
                    } catch (Exception e) {
                        wait.exception(e);
                    }
                }
            }
        });
        return wait.await(timeout);
    }

    private static class Wait<T> {

        private Thread runner;

        private T result;
        private Exception exception;
        private boolean canceled;

        public synchronized boolean runner(Thread runner) {
            this.runner = runner;
            return canceled;
        }

        public synchronized void result(T result) {
            this.result = result;
            notifyAll();
        }

        public synchronized void exception(Exception exception) {
            this.exception = exception;
            notifyAll();
        }

        public synchronized T await(long timeout) throws Exception {
            long remain = timeout;
            long prev = System.currentTimeMillis();
            try {
                for (;remain > 0 && result == null && exception == null; prev = System.currentTimeMillis()) {
                    wait(remain);
                    remain -= System.currentTimeMillis() - prev;
                }
                if (exception != null) {
                    throw exception;
                }
                if (result == null && runner != null) {
                    runner.interrupt();
                }
                return result;
            } finally {
                canceled = true;
            }
        }
    }

    private JobClient createJobClient(WorkflowJob workflow, JobConf jobConf) throws HadoopAccessorException {
        String user = workflow.getUser();
        return Services.get().get(HadoopAccessorService.class).createJobClient(user, jobConf);
    }

    private class Executor implements Runnable {

        final String actionID;

        volatile Exception ex;
        volatile boolean completed;

        public Executor(String actionID) {
            this.actionID = actionID;
        }

        @Override
        public void run() {
            LOG.debug("Executing hive query : " + query.getQueryId());
            try {
                execute();
            } catch (Exception e) {
                LOG.warn("Failed to execute query {0}", query.getQueryId(), e);
                this.ex = e;
            } finally {
                LOG.debug("Executed " + query.getQueryId() + " with " + resultCode());
                completed = true;
                CallableQueueService service = Services.get().get(CallableQueueService.class);
                service.queue(new ActionCheckXCommand(actionID));
            }
        }

        public String resultCode() {
            return ex == null ? "SUCCESS" : "FAIL";
        }

        private void execute() throws HiveServerException, TException {
            try {
                client.run();
                for (String result : client.fetchN(maxFetch)) {
                    LOG.info(result);
                }
            } finally {
                try {
                    client.clean();
                } catch (Exception e) {
                    LOG.info("Failed to cleanup hive connection", e);
                }
            }
        }
    }
}
