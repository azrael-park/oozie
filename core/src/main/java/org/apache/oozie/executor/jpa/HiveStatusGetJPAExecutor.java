package org.apache.oozie.executor.jpa;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.HiveQueryStatusBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

public class HiveStatusGetJPAExecutor implements JPAExecutor<List<HiveQueryStatusBean>> {

    private String wfId;
    private String actionName;
    private String queryId;
    private String stageId;
    private String jobId;
    private boolean hasFailedTasks;

    private String query;

    public HiveStatusGetJPAExecutor(String wfId) {
        this.wfId = wfId;
        this.query = "GET_STATUS_WF";
    }

    public HiveStatusGetJPAExecutor(String wfId, String action) {
        this.wfId = wfId;
        this.actionName = action;
        this.query = "GET_STATUS_WF_ACTION";
    }

    public HiveStatusGetJPAExecutor(String wfId, String action, String queryId) {
        this.wfId = wfId;
        this.actionName = action;
        this.queryId = queryId;
        this.query = "GET_STATUS_WF_ACTION_QUERY";
    }

    public HiveStatusGetJPAExecutor(String wfId, String action, String queryId, String stageId) {
        this.wfId = wfId;
        this.actionName = action;
        this.queryId = queryId;
        this.stageId = stageId;
        this.query = "GET_STATUS_WF_ACTION_QUERY_STAGE";
    }

    public HiveStatusGetJPAExecutor(String jobId, boolean dummy) {
        this.jobId = jobId;
        this.query = "GET_STATUS_JOB";
    }

    public void setHasFailedTasks(boolean hasFailedTasks) {
        this.hasFailedTasks = hasFailedTasks;
    }

    public String getName() {
        return "HiveStatusGetJPAExecutor";
    }

    @SuppressWarnings("unchecked")
    public List<HiveQueryStatusBean> execute(EntityManager em) throws JPAExecutorException {
        if (hasFailedTasks) {
            query += "_FAILED";
        }
        try {
            Query q = em.createNamedQuery(query);
            if (wfId != null) {
                q.setParameter("wfId", wfId);
            }
            if (actionName != null) {
                q.setParameter("actionName", actionName);
            }
            if (queryId != null) {
                q.setParameter("queryId", queryId);
            }
            if (stageId != null) {
                q.setParameter("stageId", stageId);
            }
            if (jobId != null) {
                q.setParameter("jobId", jobId);
            }
            return q.getResultList();
        } catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

}