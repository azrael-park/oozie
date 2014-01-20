package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.command.wf.ActionCheckXCommand;
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.HiveAccessService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// for hive action, create for each action
public class HiveSession extends HiveStatus {

    private final HiveTClient client;
    private final String[] queries;
    private final Executor executor;
    private final int maxFetch;

    private int index;

    public HiveSession(String wfID, String actionName, boolean monitoring, HiveTClient client, String[] queries, int maxFetch) {
        super(wfID, actionName, monitoring);
        this.client = client;
        this.queries = queries;
        this.maxFetch = maxFetch;
        this.executor = new Executor(XLog.Info.get());
    }

    private synchronized boolean hasMore() {
        return !executor.killed && index < queries.length;
    }

    private synchronized boolean isCompleted() {
        return !hasMore() && executor.executed;
    }

    public synchronized void execute(ActionExecutor.Context context, WorkflowAction action) throws Exception {
        if (hasMore()) {
            executor.configure(context, action, index++);
            Services.get().get(CallableQueueService.class).queue(executor);
        } else {
            cleanup(context, "OK");
        }
    }

    private synchronized void executeNext(ActionExecutor.Context context, WorkflowAction action) {
        CallableQueueService executors = Services.get().get(CallableQueueService.class);
        if (executor.ex == null && hasMore()) {
            executor.configure(context, action, index++);
            executors.queue(executor);
        } else {
            executors.queue(new ActionCheckXCommand(actionID));
        }
    }

    public synchronized void check(ActionExecutor.Context context) throws Exception {
        if (executor.ex != null) {
            cleanup(context, "FAILED");
            LOG.info("failed to execute query {0}", executor.toString(), executor.ex);
            throw executor.ex;
        }
        if (isCompleted()) {
            cleanup(context, executor.killed ? "KILLED" : "OK");
        }
    }

    @Override
    public synchronized void callback(String queryID, String stageID, String jobID, String jobStatus) {
        if (executor != null && executor.inverted != null && jobStatus.equals("SUCCEEDED")) {
            executor.finishPrev(queryID, stageID);
        }
        super.callback(queryID, stageID, jobID, jobStatus);
    }

    @Override
    public synchronized boolean shutdown(boolean internal) {
        super.shutdown(internal);
        executor.killed = true;
        killHiveSession();
        if (internal) {
            // remove from map
            Services.get().get(HiveAccessService.class).unregister(actionID);
        }
        return true;
    }

    private void killHiveSession() {
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.debug("Failed to shutdown hive connection", e);
        }
    }

    private void cleanup(ActionExecutor.Context context, String status) {
        LOG.info("Cleaning up hive session with status " + status);
        context.setExecutionData(status, null);   // induce ActionEndXCommand
        shutdown(true);
        LOG.info("Cleaned up hive session");
    }

    private class Executor implements Runnable {

        final XLog.Info logInfo;

        ActionExecutor.Context context;
        WorkflowAction action;
        int current;

        String queryID;
        Map<String, Set<String>> inverted;

        volatile Exception ex;
        volatile boolean executed;
        volatile boolean killed;

        public Executor(XLog.Info logInfo) {
            this.logInfo = logInfo;
        }

        public void configure(ActionExecutor.Context context, WorkflowAction action, int current) {
            this.context = context;
            this.action = action;
            this.current = current;
            this.executed = false;
        }

        @Override
        public void run() {
            XLog.Info.get().setParameters(logInfo);
            LOG.info("Executing query " + this);
            try {
                executeSQL();
            } catch (Exception e) {
                ex = e;
                if (killed) {
                    LOG.info("Failed to execute query {0} cause the action is killed", this);
                } else {
                    LOG.warn("Failed to execute query {0}", this, e);
                }
            } finally {
                LOG.info("Executed " + this + " with " + resultCode());
                if (Thread.currentThread().isInterrupted()) {
                    LOG.debug("Thread was interrupted");
                }
                executed = true;
                executeNext(context, action);
            }
        }

        private boolean executeSQL() throws Exception {

            Query plan = client.compile(queries[current]);
            if (plan == null) {
                LOG.info("Query " + this + " is not SQL command");
                client.clear();
                return false;
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(plan.getStageGraph());
            }

            queryID = plan.getQueryId();
            inverted = invertMapping(plan);

            boolean containsMR = false;
            List<Stage> stages = plan.getStageList();
            if (stages != null && !stages.isEmpty()) {
                LOG.info("Preparing for query " + plan.getQueryId());
                Set<String> walked = new HashSet<String>();
                for (Stage stage : stages) {
                    if (!walked.add(stage.getStageId())) {
                        continue;
                    }
                    StageType stageType = stage.getStageType();
                    boolean mapreduce = stageType == StageType.MAPRED;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(stage.toString());
                    }
                    LOG.info(" #" + (current + 1) + ":" + stage.getStageId() + " type " + stageType);
                    updateStatus(plan.getQueryId(), stage.getStageId(),
                            "NOT_ASSIGNED", mapreduce ? "NOT_STARTED" : stageType.name());
                    containsMR |= mapreduce;
                }
                if (containsMR) {
                    String callback = context.getCallbackUrl("$jobStatus") +
                            "jobId=$jobId&stageId=$stageId&queryId=" + plan.getQueryId();
                    LOG.debug("Oozie callback handler = " + callback);
                    client.executeTransient("set hiveconf:task.notification.url=" + callback);
                }
            }
            client.execute();

            if (LOG.isInfoEnabled()) {
                StringBuilder builder = new StringBuilder().append("fetch dump\n");
                for (String result : client.fetchN(maxFetch)) {
                    builder.append(result).append('\n');
                }
                LOG.info(builder.toString());
            }
            client.clear();

            if (stages != null && !stages.isEmpty()) {
                for (Stage stage : stages) {
                    updateStatus(plan.getQueryId(), stage.getStageId(), null, "SUCCEEDED");
                }
            }

            return containsMR;
        }

        private String resultCode() {
            return ex == null && !killed ? "SUCCEEDED" : killed ?  "KILLED" : "FAILED";
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
            if (LOG.isDebugEnabled()) {
                LOG.debug("Inverted mapping " + inverted);
            }
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

        @Override
        public String toString() {
            return (current + 1) + "/" + queries.length;
        }
    }

}