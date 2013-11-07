package org.apache.oozie.command.wf;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowActionInfo;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowActionsGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import java.util.List;
import java.util.Map;

public class ActionsXCommand extends WorkflowXCommand<WorkflowActionInfo> {

    private final Map<String, List<String>> filter;
    private final int start;
    private final int len;

    /**
     * Constructor taking the filter information
     *
     * @param filter Can be name, status, user, group and combination of these
     * @param start starting from this index in the list of workflows matching the filter are returned
     * @param length number of workflows to be returned from the list of workflows matching the filter and starting from
     *        index "start".
     */
    public ActionsXCommand(Map<String, List<String>> filter, int start, int length) {
        super("action.info", "action.info", 1, true);
        this.filter = filter;
        this.start = start;
        this.len = length;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected WorkflowActionInfo execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            //FIXME cast
            WorkflowActionInfo actions = (WorkflowActionInfo) jpaService.execute(new WorkflowActionsGetJPAExecutor(filter, start, len));
            for (WorkflowActionBean action : actions.getActions()) {
                action.setConsoleUrl(JobXCommand.getJobConsoleUrl(action.getId()));
            }
            return actions;
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    public String getEntityKey() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
