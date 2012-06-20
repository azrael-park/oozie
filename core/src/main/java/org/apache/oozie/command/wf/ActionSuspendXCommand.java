package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.hive.HiveStatus;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.HiveAccessService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.LogUtils;

public class ActionSuspendXCommand<T> extends WorkflowXCommand<T> {

    private String jobId;
    private String actionId;

    private WorkflowJobBean wfJob;
    private WorkflowActionBean wfAction;
    private JPAService jpaService;

    public ActionSuspendXCommand(String actionId) {
        super("action.suspend", "suspend", 1);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
    }

    protected boolean isLockRequired() {
        return true;
    }

    public String getEntityKey() {
        return jobId;
    }

    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
                this.wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(actionId));
                LogUtils.setLogInfo(wfJob, logInfo);
                LogUtils.setLogInfo(wfAction, logInfo);
            } else {
                throw new CommandException(ErrorCode.E0610);
            }
        } catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (wfJob.getStatus() == WorkflowJob.Status.KILLED ||
                wfJob.getStatus() == WorkflowJob.Status.FAILED ||
                wfJob.getStatus() == WorkflowJob.Status.SUCCEEDED) {
            throw new PreconditionException(ErrorCode.E0831, wfJob.getStatus());
        }
        if (wfAction.getStatus() == WorkflowAction.Status.KILLED ||
                wfAction.getStatus() == WorkflowAction.Status.FAILED ||
                wfAction.getStatus() == WorkflowAction.Status.ERROR ||
                wfAction.getStatus() == WorkflowAction.Status.OK) {
            throw new PreconditionException(ErrorCode.E0822, wfAction.getStatus());
        }
    }

    protected T execute() throws CommandException {
        wfAction.resetPendingOnly();
        wfAction.setStatus(WorkflowAction.Status.START_MANUAL);
        try {
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
        } catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        HiveAccessService access = Services.get().get(HiveAccessService.class);
        HiveStatus session = access == null ? null : access.peekRunningStatus(actionId);
        if (session != null) {
            try {
                session.shutdown();
            } catch (Exception e) {
                LOG.info("failed to kill running hive session", e);
            }
        }
        return null;
    }
}
