package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.jdom.JDOMException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.oozie.client.WorkflowAction.Status.PREP;
import static org.apache.oozie.client.WorkflowAction.Status.START_MANUAL;

public class ActionUpdateXCommand extends ActionXCommand<Void> {

    private Map<String, String> updates;

    public ActionUpdateXCommand(String actionId, Map<String, String> updates) {
        super(actionId, "action.update", "update", 0);
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
        if (wfAction.getStatus() != START_MANUAL && wfAction.getStatus() != PREP) {
            throw new PreconditionException(ErrorCode.E0826, wfAction.getStatus());
        }
        loadActionExecutor(false);
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("Updating action with " + updates);
        try {
            if (!updates.isEmpty()) {
                executor.updateAttributes(wfAction, updates);
            }
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
        } catch (XException e) {
            throw new CommandException(e);
        } catch (JDOMException e) {
            throw new CommandException(ErrorCode.E0700, wfAction.getType());
        } catch (Exception e) {
            throw new CommandException(ErrorCode.E0829, e.toString());
        }
        LOG.info("Action is updated successfully");
        return null;
    }
}
