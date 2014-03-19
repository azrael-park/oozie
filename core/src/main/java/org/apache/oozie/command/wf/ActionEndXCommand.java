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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.DagELFunctions;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.control.ControlNodeActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.db.SLADbXOperations;
import org.apache.oozie.workflow.WorkflowInstance;

@SuppressWarnings("deprecation")
public class ActionEndXCommand extends ActionXCommand<Void> {
    public static final String COULD_NOT_END = "COULD_NOT_END";
    public static final String END_DATA_MISSING = "END_DATA_MISSING";

    private List<JsonBean> updateList = new ArrayList<JsonBean>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();

    public ActionEndXCommand(String actionId, String type) {
        super(actionId, "action.end", type, 0);
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
    public String getKey() {
        return getName() + "_" + actionId;
    }

    @Override
    protected void loadState() throws CommandException {
        loadActionBean();
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        verifyActionBean();
        if (wfAction.isPending()
                && (wfAction.getStatus() == WorkflowActionBean.Status.DONE
                        || wfAction.getStatus() == WorkflowActionBean.Status.END_RETRY || wfAction.getStatus() == WorkflowActionBean.Status.END_MANUAL)) {

            if (wfJob.getStatus() != WorkflowJob.Status.RUNNING) {
                throw new PreconditionException(ErrorCode.E0811,  WorkflowJob.Status.RUNNING.toString());
            }
        }
        else {
            throw new PreconditionException(ErrorCode.E0812, wfAction.getPending(), wfAction.getStatusStr());
        }
        loadActionExecutor(true);
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.info("STARTED ActionEndXCommand : status[{0}]", wfAction.getStatus());

        Configuration conf = wfJob.getWorkflowInstance().getConf();

        int maxRetries = 0;
        long retryInterval = 0;

        if (!(executor instanceof ControlNodeActionExecutor)) {
            maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
            retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
        }

        executor.setMaxRetries(maxRetries);
        executor.setRetryInterval(retryInterval);

        boolean isRetry = false;
        if (wfAction.getStatus() == WorkflowActionBean.Status.END_RETRY
                || wfAction.getStatus() == WorkflowActionBean.Status.END_MANUAL) {
            isRetry = true;
        }
        boolean isUserRetry = false;
        ActionExecutorContext context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
        try {

            execute(context);
            onSuccess(context);
        }
        catch (Throwable ex) {
            onFailure(context, ex);
        }
        finally {
            try {
                jpaService.execute(new BulkUpdateInsertJPAExecutor(updateList, insertList));
                if (!(executor instanceof ControlNodeActionExecutor) && EventHandlerService.isEnabled()) {
                    generateEvent(wfAction, wfJob.getUser());
                }
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            queue(new SignalXCommand(jobId, actionId));
        }

        LOG.info("ENDED ActionEndXCommand : status[{0}]", wfAction.getStatus());

        return null;
    }

    private void execute(ActionExecutorContext context) throws ActionExecutorException {
        LOG.debug("End, name [{0}] type [{1}] status[{2}] external status [{3}] signal value [{4}]",
                wfAction.getName(), wfAction.getType(), wfAction.getStatus(), wfAction.getExternalStatus(),
                wfAction.getSignalValue());

        Instrumentation.Cron cron = new Instrumentation.Cron();
        cron.start();
        executor.end(context, wfAction);
        cron.stop();
        addActionCron(wfAction.getType(), cron);
    }

    private void onSuccess(ActionExecutorContext context) throws XException {
        WorkflowInstance wfInstance = wfJob.getWorkflowInstance();
        DagELFunctions.setActionInfo(wfInstance, wfAction);
        wfJob.setWorkflowInstance(wfInstance);
        incrActionCounter(wfAction.getType(), 1);

        if (!context.isEnded()) {
            LOG.warn(XLog.OPS, "Action Ended, ActionExecutor [{0}] must call setEndData()", executor.getType());
            wfAction.setErrorInfo(END_DATA_MISSING, "Execution Ended, but End Data Missing from Action");
            failJob(context);
        } else {
            wfAction.setRetries(0);
            wfAction.setEndTime(new Date());

            boolean shouldHandleUserRetry = false;
            Status slaStatus = null;
            switch (wfAction.getStatus()) {
                case OK:
                    slaStatus = Status.SUCCEEDED;
                    break;
                case KILLED:
                    slaStatus = Status.KILLED;
                    break;
                case FAILED:
                    slaStatus = Status.FAILED;
                    shouldHandleUserRetry = true;
                    break;
                case ERROR:
                    LOG.info("ERROR is considered as FAILED for SLA");
                    slaStatus = Status.KILLED;
                    shouldHandleUserRetry = true;
                    break;
                default:
                    slaStatus = Status.FAILED;
                    shouldHandleUserRetry = true;
                    break;
            }
            if (!shouldHandleUserRetry || !handleUserRetry(wfAction)) {
                SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(), slaStatus, SlaAppType.WORKFLOW_ACTION);
                LOG.info("Queuing signal commands, status=" + wfAction.getStatus() + ", Set pending=" + wfAction.getPending());
                if(slaEvent != null) {
                    insertList.add(slaEvent);
                }
            }

        }
        updateList.add(wfAction);
        wfJob.setLastModifiedTime(new Date());
        updateList.add(wfJob);
    }

    protected void onFailure(ActionExecutorContext context, Throwable t) throws CommandException {
        if (t instanceof ActionExecutorException) {
            onActionException(context, (ActionExecutorException) t);
        } else {
            divergeOnError(context, ActionExecutorException.ErrorType.NON_TRANSIENT);
            LOG.warn("Unhandled exception", t);
        }
    }

    private void onActionException(ActionExecutorContext context, ActionExecutorException ex) throws CommandException {
        LOG.info("Action execution exception. ErrorType [{0}], ErrorCode [{1}], Message [{2}]",
                ex.getErrorType(), ex.getErrorCode(), ex.getMessage(), ex);
        wfAction.setErrorInfo(ex.getErrorCode(), ex.getMessage());
        wfAction.setEndTime(null);

        divergeOnError(context, ex.getErrorType());
    }

    private void divergeOnError(ActionExecutorContext context, ActionExecutorException.ErrorType type) throws CommandException {
        LOG.info("DivergeOnStatus, wfAction.status [{0}]", wfAction.getStatus());
        switch (type) {
            case TRANSIENT:
                handleTransient(context, executor, WorkflowAction.Status.END_RETRY);
                wfAction.setEndTime(null);
                break;
            case NON_TRANSIENT:
                handleNonTransient(context, executor, WorkflowAction.Status.END_MANUAL);
                wfAction.setEndTime(null);
                break;
            case ERROR:
                handleError(context, executor, COULD_NOT_END, WorkflowAction.Status.ERROR);
                queue(new SignalXCommand(jobId, actionId));
                break;
            case FAILED:
                failJob(context);
                break;
        }

        WorkflowInstance wfInstance = wfJob.getWorkflowInstance();
        DagELFunctions.setActionInfo(wfInstance, wfAction);
        wfJob.setWorkflowInstance(wfInstance);

        updateList.add(wfAction);
        wfJob.setLastModifiedTime(new Date());
        updateList.add(wfJob);
    }

    @Override
    protected boolean handleTransient(ActionExecutor.Context context, ActionExecutor executor,
                                      WorkflowAction.Status status) throws CommandException {
        if (!super.handleTransient(context, executor, status)) {
            handleNonTransient(context, executor, WorkflowAction.Status.END_MANUAL);
            wfAction.setPendingAge(new Date());
            wfAction.setRetries(0);
            return false;
        }
        return true;
    }

    @Override
    protected boolean handleError(ActionExecutor.Context context, ActionExecutor executor, String message,
                                  WorkflowAction.Status status) throws CommandException {
        if (!super.handleError(context, executor, message, status)) {
            wfAction.setPending();
            wfAction.setEndData(status, WorkflowAction.Status.ERROR.toString());
            return false;
        }
        return true;
    }

}
