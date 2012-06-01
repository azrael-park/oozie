package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.LogUtils;
import org.jdom.JDOMException;

import java.util.Map;

import static org.apache.oozie.client.WorkflowAction.Status.START_MANUAL;

public class ActionUpdateXCommand extends ActionXCommand<Void> {

    private String jobId;
    private String actionId;

    private ActionExecutor executor;
    private WorkflowActionBean wfAction;

    private Map<String, String> updates;
    private JPAService jpaService;

    public ActionUpdateXCommand(String actionId, Map<String, String> updates) {
        super("action.update", "action.update", 0);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
        this.updates = updates;
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    public String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(actionId));
                LogUtils.setLogInfo(wfAction, logInfo);
            } else {
                throw new CommandException(ErrorCode.E0610);
            }
        } catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (wfAction == null) {
            throw new PreconditionException(ErrorCode.E0605, actionId);
        }
        if (wfAction.getStatus() != START_MANUAL) {
            throw new PreconditionException(ErrorCode.E0826, wfAction.getStatus());
        }
        executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
        if (executor == null) {
            throw new CommandException(ErrorCode.E0802, wfAction.getType());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("updating action with " + updates);
        try {
            executor.updateAttributes(wfAction, updates);
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
        } catch (XException e) {
            throw new CommandException(e);
        } catch (JDOMException e) {
            throw new CommandException(ErrorCode.E0700, wfAction.getType());
        } catch (Exception e) {
            throw new CommandException(ErrorCode.E0829, e.toString());
        }
        LOG.info("Action update successfully");
        return null;
    }
}
