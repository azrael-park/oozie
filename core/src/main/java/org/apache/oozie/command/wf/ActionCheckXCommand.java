/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.command.wf;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.service.ActionCheckerService;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;

/**
 * Executes the check command for ActionHandlers. </p> Ensures the action is in
 * RUNNING state before executing
 * {@link ActionExecutor#check(org.apache.oozie.action.ActionExecutor.Context, org.apache.oozie.client.WorkflowAction)}
 */
public class ActionCheckXCommand extends ActionXCommand {
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";

    private int actionCheckDelay;
    private List<JsonBean> updateList = new ArrayList<JsonBean>();
    private boolean generateEvent = false;

    public ActionCheckXCommand(String actionId) {
        this(actionId, -1);
    }

    public ActionCheckXCommand(String actionId, int checkDelay) {
        this(actionId, 0, checkDelay);
    }

    public ActionCheckXCommand(String actionId, int priority, int checkDelay) {
        super(actionId, "action.check", "check", priority);
        this.actionCheckDelay = checkDelay;
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
        if (actionCheckDelay > 0) {
            Timestamp actionCheckTs = new Timestamp(System.currentTimeMillis() - actionCheckDelay * 1000);
            Timestamp actionLmt = wfAction.getLastCheckTimestamp();
            if (actionLmt.after(actionCheckTs)) {
                throw new PreconditionException(ErrorCode.E0817, actionId);
            }
        }
        if (!wfAction.isPending() || wfAction.getStatus() != WorkflowActionBean.Status.RUNNING) {
            throw new PreconditionException(ErrorCode.E0815, wfAction.getPending(), wfAction.getStatusStr());
        }
        if (wfJob.getStatus() != WorkflowJob.Status.RUNNING && wfJob.getStatus() != WorkflowJob.Status.SUSPENDED) {
            wfAction.setLastCheckTime(new Date());
            try {
                jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            throw new PreconditionException(ErrorCode.E0818, wfAction.getId(), wfJob.getId(), wfJob.getStatus());
        }
        loadActionExecutor(false);
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED ActionCheckXCommand : status[{0}] external status [{1}] signal value [{2}]",
                wfAction.getStatus(), wfAction.getExternalStatus(), wfAction.getSignalValue());

        long retryInterval = Services.get().getConf().getLong(ActionCheckerService.CONF_ACTION_CHECK_INTERVAL, executor
                .getRetryInterval());
        executor.setRetryInterval(retryInterval);

        ActionExecutorContext context = null;
        try {
            boolean isRetry = false;
            if (wfAction.getRetries() > 0) {
                isRetry = true;
            }
            boolean isUserRetry = false;
            context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
            incrActionCounter(wfAction.getType(), 1);

            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            executor.check(context, wfAction);
            cron.stop();
            addActionCron(wfAction.getType(), cron);

            if (wfAction.isExecutionComplete()) {
                if (!context.isExecuted()) {
                    LOG.warn(XLog.OPS, "Action Completed, ActionExecutor [{0}] must call setExecutionData()", executor
                            .getType());
                    wfAction.setErrorInfo(EXEC_DATA_MISSING,
                            "Execution Complete, but Execution Data Missing from Action");
                    failJob(context);
                    generateEvent = true;
                    sendActionNotification();
                } else {
                    wfAction.setPending();
                    queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
                }
            }
            wfAction.setLastCheckTime(new Date());
            updateList.add(wfAction);
            wfJob.setLastModifiedTime(new Date());
            updateList.add(wfJob);
        }
        catch (ActionExecutorException ex) {
            LOG.warn("Exception while executing check(). Error Code [{0}], Type [{1}], Message[{2}]",
                    ex.getErrorCode(), ex.getErrorType(), ex.getMessage(), ex);

            wfAction.setErrorInfo(ex.getErrorCode(), ex.getMessage());
            switch (ex.getErrorType()) {
                case FAILED:
                    failJob(context, wfAction);
                    generateEvent = true;
                    break;
                case ERROR:
                    handleError(context, executor, "", wfAction.getStatus());
                    break;
                case TRANSIENT:                 // retry N times, then suspend workflow
                    if (!handleTransient(context, executor, WorkflowAction.Status.RUNNING)){
                         generateEvent = true;
                    }
                    break;
                case NON_TRANSIENT:
                    handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                    break;
            }
            wfAction.setLastCheckTime(new Date());
            updateList = new ArrayList<JsonBean>();
            updateList.add(wfAction);
            wfJob.setLastModifiedTime(new Date());
            updateList.add(wfJob);
        }
        finally {
            try {
                jpaService.execute(new BulkUpdateInsertJPAExecutor(updateList, null));
                if (generateEvent && EventHandlerService.isEnabled()) {
                    generateEvent(wfAction, wfJob.getUser());
                }
                sendActionNotification();
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
        }

        LOG.info("ENDED ActionCheckXCommand : status[{0}]", wfAction.getStatus());
        return null;
    }

    @Override
    protected boolean handleTransient(ActionExecutor.Context context, ActionExecutor executor,
                                      WorkflowAction.Status status) throws CommandException {
        if (!super.handleTransient(context, executor, WorkflowAction.Status.RUNNING)) {
            handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
            wfAction.setPendingAge(new Date());
            wfAction.setRetries(0);
            wfAction.setStartTime(null);
            return false;
        }
        return true;
    }

    protected long getRetryInterval() {
        return (executor != null) ? executor.getRetryInterval() : ActionExecutor.RETRY_INTERVAL;
    }

    @Override
    public String getKey() {
        return getName() + "_" + actionId;
    }

}
