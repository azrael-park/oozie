package org.apache.oozie.service;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionsRunningInWorkFlowGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobsRunningJPAExecutor;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

import javax.persistence.EntityManager;

public class JobRecoveryService implements Service {

    @Override
    public void init(Services services) throws ServiceException {
        JPAService jpa = services.get(JPAService.class);
        EntityManager manager = jpa.getEntityManager();

        try {
            manager.getTransaction().begin();
            try {
                for (WorkflowJobBean workflow : jpa.execute(new WorkflowJobsRunningJPAExecutor())) {
                    workflow.setStatus(WorkflowJob.Status.SUSPENDED);
                    LiteWorkflowInstance instance = (LiteWorkflowInstance) workflow.getWorkflowInstance();
                    instance.setStatus(WorkflowInstance.Status.SUSPENDED);
                    workflow.setWfInstance(instance);
                    jpa.execute(new WorkflowJobUpdateJPAExecutor(workflow));
                    for (WorkflowActionBean action : jpa.execute(new WorkflowActionsRunningInWorkFlowGetJPAExecutor(workflow.getId()))) {
                        action.resetPending();
                        action.setStatus(WorkflowAction.Status.START_MANUAL);
                        jpa.execute(new WorkflowActionUpdateJPAExecutor(action));
                    }
                }
            } catch (Throwable e) {
                manager.getTransaction().rollback();
                convert(e);
            }
            manager.getTransaction().commit();
        } finally {
            manager.close();
        }
    }

    @Override
    public void destroy() {
    }

    @Override
    public Class<? extends Service> getInterface() {
        return JobRecoveryService.class;
    }

    protected ServiceException convert(Throwable t) {
        if (t instanceof ServiceException) {
            return (ServiceException) t;
        }
        if (t instanceof XException) {
            return new ServiceException((XException) t);
        }
        return new ServiceException(ErrorCode.E9999, t);
    }
}
