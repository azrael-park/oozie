package org.apache.oozie.executor.jpa;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;

public class WorkflowJobsRunningJPAExecutor implements JPAExecutor<List<WorkflowJobBean>> {

    public String getName() {
        return "WorkflowJobsGetForRecoveryJPAExecutor";
    }

    @SuppressWarnings("unchecked")
    public List<WorkflowJobBean> execute(EntityManager em) throws JPAExecutorException {
        try {
            Query jobQ = em.createNamedQuery("GET_WORKFLOWS_UNFINISHED");
            return jobQ.getResultList();
        } catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e);
        }
    }
}