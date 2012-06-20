package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Adjacency;
import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.QueryPlan;
import org.apache.hadoop.hive.ql.plan.api.Stage;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
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

import static org.apache.oozie.action.ActionExecutorException.ErrorType.FAILED;

// for hive action, create for each action
public class HiveSession extends HiveStatus {

    final ThriftHive.Client client;
    final String[] queries;
    final int maxFetch;

    int index;
    Executor executor;

    boolean killed;

    public HiveSession(String wfID, String actionName, ThriftHive.Client client, String[] queries, int maxFetch) {
        super(wfID, actionName);
        this.client = client;
        this.queries = queries;
        this.maxFetch = maxFetch;
    }

    private synchronized boolean isFinal() {
        return killed || index >= queries.length;
    }

    private synchronized boolean isCompleted() {
        return isFinal() && (executor == null || executor.executed || executor.ex != null);
    }

    public synchronized void execute(ActionExecutor.Context context, WorkflowAction action) throws Exception {
        if (!executeNext(context, action)) {
            check(context);
        }
    }

    private synchronized boolean executeNext(ActionExecutor.Context context, WorkflowAction action) {
        if (!isFinal()) {
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
            throw new ActionExecutorException(FAILED,
                    "HIVE-002", "failed to execute query {0}", executor.toString(), executor.ex);
        }
        if (isCompleted()) {
            cleanup(context, "OK");
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
    public synchronized boolean shutdown() {
        super.shutdown();
        killed = true;
        closeSession();
        HiveAccessService hive = Services.get().get(HiveAccessService.class);
        if (hive != null) {
            hive.actionFinished(actionID);
        }
        return executor != null;
    }

    private void closeSession() {
        try {
            client.shutdown();
        } catch (Exception e) {
            LOG.debug("Failed to shutdown hive connection", e);
        }
    }

    private void cleanup(ActionExecutor.Context context, String status) {
        LOG.info("Cleaning up hive session");
        context.setExecutionData(status, null);   // induce ActionEndXCommand
        shutdown();
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
