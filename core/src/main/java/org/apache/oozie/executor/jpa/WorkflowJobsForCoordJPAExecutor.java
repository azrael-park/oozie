package org.apache.oozie.executor.jpa;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.ParamChecker;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class WorkflowJobsForCoordJPAExecutor implements JPAExecutor<List<WorkflowJobBean>> {

    private String coordId;

    public WorkflowJobsForCoordJPAExecutor(String coordId) {
        ParamChecker.notNull(coordId, "coordId");
        this.coordId = coordId;
    }

    public String getName() {
        return "WorkflowJobsBelongToCoordJPAExecutor";
    }

    @SuppressWarnings("unchecked")
    public List<WorkflowJobBean> execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createQuery(WorkflowsJobGetJPAExecutor.seletStr + " WHERE parent_id LIKE " + coordId + "%");
            List<Object[]> results = (List<Object[]>) q.getResultList();
            return WorkflowsJobGetJPAExecutor.getBeansForWorkflowFromArray(results);
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }


}

