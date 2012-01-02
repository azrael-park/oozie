package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.NodeDef;

import static org.apache.oozie.client.WorkflowAction.Status.ERROR;
import static org.apache.oozie.client.WorkflowAction.Status.FAILED;
import static org.apache.oozie.client.WorkflowAction.Status.KILLED;

public class ActionResumeXCommand extends WorkflowXCommand<Void> {

    private String jobId;
    private String actionId;
    private WorkflowJobBean wfJob;
    private WorkflowActionBean wfAction;
    private JPAService jpaService;

    public ActionResumeXCommand(String actionId) {
        super("action.resume", "resume", 1);
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
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        try {
            wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
            wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(actionId));
            LogUtils.setLogInfo(wfJob, logInfo);
            LogUtils.setLogInfo(wfAction, logInfo);
        } catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (wfJob.getStatus() != WorkflowJob.Status.SUSPENDED && wfJob.getStatus() != WorkflowJob.Status.RUNNING) {
            throw new PreconditionException(ErrorCode.E0823, wfJob.getStatus());
        }
        if (wfAction.getStatus() != WorkflowAction.Status.START_MANUAL) {
            throw new PreconditionException(ErrorCode.E0824, wfAction.getStatus());
        }
    }

    protected Void execute() throws CommandException {
        if (wfJob.getStatus() == WorkflowJob.Status.SUSPENDED) {
            wfAction.resetPending();
            wfAction.setStatus(WorkflowAction.Status.PREP);
            updateActionStatus(wfAction);
            return null;
        }
        NodeDef headNode = executionHead();
        boolean start = wfAction.getName().equals(headNode.getName());
        if (!start) {
            String headID = Services.get().get(UUIDService.class).generateChildId(wfJob.getId(), headNode.getName());
            try {
                WorkflowActionBean head = jpaService.execute(new WorkflowActionGetJPAExecutor(headID));
                start = head.getStatus() != FAILED && head.getStatus() != KILLED && head.getStatus() != ERROR && !head.isPending();
            } catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            wfAction.setStatus(WorkflowAction.Status.PREP);
        }
        if (start) {
            wfAction.setPendingOnly();
            queue(new ActionStartXCommand(wfAction.getId(), wfAction.getType()));
        }
        updateActionStatus(wfAction);
        return null;
    }

    private void updateActionStatus(WorkflowActionBean action) throws CommandException {
        try {
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(action));
        } catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    private NodeDef executionHead() throws CommandException {
        return executionHead(wfJob.getWorkflowInstance(), wfAction.getExecutionPath());
    }

    private NodeDef executionHead(WorkflowInstance instance, String executionPath) {
        if (executionPath != null) {
            NodeDef node = instance.getNodeDef(executionPath);
            return node != null ? node : executionHead(instance, LiteWorkflowInstance.getParentPath(executionPath));
        }
        return null;
    }
}
