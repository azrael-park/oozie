package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.service.HiveAccessService;
import org.apache.oozie.service.Services;

public class ActionSuspendXCommand extends ActionXCommand<Void> {

    public ActionSuspendXCommand(String actionId) {
        super(actionId, "action.suspend", "suspend", 1);
    }

    protected boolean isLockRequired() {
        return true;
    }

    public String getEntityKey() {
        return jobId;
    }

    protected void loadState() throws CommandException {
        loadActionBean();
    }

    protected void verifyPrecondition() throws CommandException, PreconditionException {
        verifyActionBean();
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

    protected Void execute() throws CommandException {
        wfAction.resetPendingOnly();
        wfAction.setStatus(WorkflowAction.Status.START_MANUAL);
        try {
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
        } catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        sendActionNotification();
        HiveAccessService access = Services.get().get(HiveAccessService.class);
        access.actionFinished(actionId);
        return null;
    }
}
