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
        if (wfJob.getStatus() != SUSPENDED && wfJob.getStatus() != RUNNING) {
            throw new PreconditionException(ErrorCode.E0823, wfJob.getStatus());
        }
        if (wfAction.getStatus() != WorkflowAction.Status.START_MANUAL &&
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

    // job should be in RUNNING or SUSPENDED status (also used by ResumeXCommand)
    static ActionXCommand resumeAction(WorkflowJobBean job, WorkflowActionBean action) {
        if (job.getStatus() != RUNNING || !isExecutionHead(job, action)) {
            action.resetPending();
            action.setStatus(WorkflowAction.Status.PREP);
            return null;
        }
        if (action.getStatus() == WorkflowAction.Status.START_MANUAL ||
                action.getStatus() == WorkflowAction.Status.PREP) {
            action.setPendingOnly();
            return new ActionStartXCommand(action.getId(), action.getType());
        }
        if (action.getStatus() == WorkflowAction.Status.END_MANUAL) {
            action.setPendingOnly();
            return new ActionEndXCommand(action.getId(), action.getType());
        }
        return null;
    }

    private static boolean isExecutionHead(WorkflowJobBean job, WorkflowActionBean action) {
        NodeDef node = executionHead(job.getWorkflowInstance(), action.getExecutionPath());
        boolean executable = node != null && node.getName().equals(action.getName());
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
