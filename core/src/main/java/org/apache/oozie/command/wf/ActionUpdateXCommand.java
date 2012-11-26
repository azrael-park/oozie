package org.apache.oozie.command.wf;

import java.util.Map;

import static org.apache.oozie.client.WorkflowAction.Status.START_MANUAL;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.jdom.JDOMException;

public class ActionUpdateXCommand extends ActionXCommand<Void> {

    private Map<String, String> updates;

    public ActionUpdateXCommand(String actionId, Map<String, String> updates) {
        super(actionId, "action.update", "action.update", 0);
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
        loadActionBean();
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        verifyActionBean();
        if (wfAction.getStatus() != START_MANUAL) {
            throw new PreconditionException(ErrorCode.E0826, wfAction.getStatus());
        }
        loadActionExecutor(false);
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
