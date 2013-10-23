package org.apache.oozie.executor.jpa;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;

import javax.persistence.EntityManager;
import javax.persistence.Query;

public class HiveStatusDeleteJPAExecutor implements JPAExecutor<Integer> {

    String wfId;
    String actionName;

    public HiveStatusDeleteJPAExecutor(String actionId) {
        UUIDService uid = Services.get().get(UUIDService.class);
        this.wfId = uid.getId(actionId);
        this.actionName = uid.getChildName(actionId);
    }

    public HiveStatusDeleteJPAExecutor(String wfId, String actionName) {
        this.wfId = wfId;
        this.actionName = actionName;
    }

    @Override
    public String getName() {
        return "HiveActionDeleteJPAExecutor";
    }

    @Override
    @SuppressWarnings("unchecked")
    public Integer execute(EntityManager em) throws JPAExecutorException {
        String query = actionName != null ? "DELETE_STATUS_WF_ACTION" : "DELETE_STATUS_WF";
        try {
            Query q = em.createNamedQuery(query);
            q.setParameter("wfId", wfId);
            if (actionName != null) {
                q.setParameter("actionName", actionName);
            }
            return q.executeUpdate();
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0603, e.getMessage(), e);
        }
    }

}