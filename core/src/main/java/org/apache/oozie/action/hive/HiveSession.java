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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// for hive action, create for each action
public class HiveSession extends HiveStatus {

    final HiveTClient client;
    final String[] queries;
    final int maxFetch;

    private int index;
    private Executor executor;

    private boolean killed;

    public HiveSession(String wfID, String actionName, boolean monitoring, HiveTClient client, String[] queries, int maxFetch) {
        super(wfID, actionName, monitoring);
        this.client = client;
        this.queries = queries;
        this.maxFetch = maxFetch;
    }

    private synchronized boolean hasMore() {
        return !killed && index < queries.length;
    }

    private synchronized boolean isCompleted() {
        return !hasMore() && (executor == null || executor.executed || executor.ex != null);
    }

    public synchronized void execute(ActionExecutor.Context context, WorkflowAction action) throws Exception {
        if (!executeNext(context, action)) {
            check(context);
        }
    }

    private synchronized boolean executeNext(ActionExecutor.Context context, WorkflowAction action) {
        if (hasMore()) {
            executor = new Executor(context, action, queries[index], ++index);
            Services.get().get(CallableQueueService.class).queue(executor);
            return true;
        }
        executor = null;
        return false;
    }

    public synchronized void check(ActionExecutor.Context context) throws Exception {
        if (executor != null && executor.ex != null) {
            cleanup(context, "FAILED");
            LOG.info("failed to execute query {0}", executor.toString(), executor.ex);
            throw executor.ex;
        }
        if (isCompleted()) {
            cleanup(context, killed ? "KILLED" : "OK");
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
        killed = true;
        killHiveSession();
        if (internal) {
            // remove from map
            Services.get().get(HiveAccessService.class).unregister(actionID);
        }
        return executor == null;
    }

    private void killHiveSession() {
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.debug("Failed to shutdown hive connection", e);
        }
    }

    private void cleanup(ActionExecutor.Context context, String status) {
        LOG.info("Cleaning up hive session");
        context.setExecutionData(status, null);   // induce ActionEndXCommand
        shutdown(true);
    }

    private class Executor implements Runnable {

        ActionExecutor.Context context;
        WorkflowAction action;
        String query;
        int current;

        String queryID;
        Map<String, Set<String>> inverted;

        volatile Exception ex;
        volatile boolean executed;

        public Executor(ActionExecutor.Context context, WorkflowAction action, String query, int current) {
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

            Query plan = client.compile(query);
            if (plan == null) {
                LOG.info("Query " + this + " is not SQL command");
                client.clear();
                return false;
            }
            LOG.debug(plan.getStageGraph());

            queryID = plan.getQueryId();
            inverted = invertMapping(plan);

            boolean containsMR = false;
            List<Stage> stages = plan.getStageList();
            if (stages != null && !stages.isEmpty()) {
                for (Stage stage : stages) {
                    StageType stageType = stage.getStageType();
                    boolean mapreduce = stageType == StageType.MAPRED || stageType == StageType.MAPREDLOCAL;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(stage.toString());
                    }
                    LOG.info("Preparing for query " + plan.getQueryId() + " in stage " + stage.getStageId() + " type " + stageType);
                    updateStatus(plan.getQueryId(), stage.getStageId(), "NOT_ASSIGNED", mapreduce ? "NOT_STARTED" : stageType.name());
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

            StringBuilder builder = new StringBuilder().append("fetch dump\n");
            for (String result : client.fetchN(maxFetch)) {
                builder.append(result).append('\n');
            }
            LOG.info(builder.toString());
            client.clear();

            if (stages != null && !stages.isEmpty()) {
                for (Stage stage : stages) {
                    updateStatus(plan.getQueryId(), stage.getStageId(), null, "SUCCEEDED");
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
