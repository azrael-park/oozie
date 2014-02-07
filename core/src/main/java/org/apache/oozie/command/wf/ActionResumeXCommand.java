package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.client.WorkflowAction;
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
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.workflow.lite.NodeDef;

import static org.apache.oozie.client.WorkflowJob.Status.RUNNING;
import static org.apache.oozie.client.WorkflowJob.Status.SUSPENDED;

public class ActionResumeXCommand extends ActionXCommand<Void> {

    public ActionResumeXCommand(String actionId) {
        super(actionId, "action.resume", "resume", 1);
    }

    protected void loadState() throws CommandException {
        loadActionBean();
    }

    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (wfJob.getStatus() != SUSPENDED && wfJob.getStatus() != RUNNING) {
            throw new PreconditionException(ErrorCode.E0823, wfJob.getStatus());
        }
        if (wfAction.getStatus() != WorkflowAction.Status.PREP &&
                wfAction.getStatus() != WorkflowAction.Status.START_MANUAL &&
                wfAction.getStatus() != WorkflowAction.Status.DONE &&
                wfAction.getStatus() != WorkflowAction.Status.END_MANUAL) {
            throw new PreconditionException(ErrorCode.E0824, wfAction.getStatus());
        }
    }

    protected Void execute() throws CommandException {
        ActionXCommand command = resumeAction(wfJob, wfAction);
        try {
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
        } catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        if (command != null) {
            LOG.info("Starting Action Name: "+ wfAction.getName() + ", Id: " + wfAction.getId() +
                    ", Authcode:" + wfAction.getCred(), ", Status:" + wfAction.getStatus());
            queue(command);
        }
        return null;
    }

    static ActionXCommand resumeAction(WorkflowJobBean job, WorkflowActionBean action) {
        return resumeAction(job, job.getWorkflowInstance(), action);
    }

    // job should be in RUNNING or SUSPENDED status (also used by ResumeXCommand)
    static ActionXCommand resumeAction(WorkflowJobBean job, WorkflowInstance instance, WorkflowActionBean action) {
        if ( !(job.getStatus() == RUNNING || job.getStatus() == SUSPENDED) || !isExecutionHead(instance, action)) {
            action.resetPending();
            action.setStatus(WorkflowAction.Status.PREP);
            return null;
        }
        if (action.getStatus() == WorkflowAction.Status.START_MANUAL ||
                action.getStatus() == WorkflowAction.Status.PREP) {
            action.setPendingOnly();
            return new ActionStartXCommand(action.getId(), action.getType());
        }
        if (action.getStatus() == WorkflowAction.Status.END_MANUAL ||
                action.getStatus() == WorkflowAction.Status.DONE) {
            action.setPendingOnly();
            return new ActionEndXCommand(action.getId(), action.getType());
        }
        return null;
    }

    private static boolean isExecutionHead(WorkflowInstance instance, WorkflowActionBean action) {
        NodeDef head = executionHead(instance, action.getExecutionPath());
        boolean executable = head != null && head.getName().equals(action.getName());
        if (executable) {
            XLog.getLog(ActionResumeXCommand.class).info(action.getName() +
                    " in path " + action.getExecutionPath() + " is head of execution");
        }
        return executable;
    }

    private static NodeDef executionHead(WorkflowInstance instance, String executionPath) {
        if (executionPath != null) {
            NodeDef node = instance.getNodeDef(executionPath);
            return node != null ? node : executionHead(instance, LiteWorkflowInstance.getParentPath(executionPath));
        }
        return null;
    }
}
