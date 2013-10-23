package org.apache.oozie.executor.jpa;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

public class WorkflowActionsRunningInWorkFlowGetJPAExecutor implements JPAExecutor<List<WorkflowActionBean>> {

    private String wfID;

    public WorkflowActionsRunningInWorkFlowGetJPAExecutor(String wfID) {
        this.wfID = wfID;
    }

    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_RUNNING_ACTIONS_FOR_WORKFLOW");
            q.setParameter("wfId", wfID);
            return q.getResultList();
        } catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0605, e);
        }
    }

    public String getName() {
        return "WorkflowActionsRunningInGetJPAExecutor";
    }
}
