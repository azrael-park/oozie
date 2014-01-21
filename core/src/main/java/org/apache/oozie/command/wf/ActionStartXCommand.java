/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

import javax.servlet.jsp.el.ELException;
import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.SLAEventBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
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
import org.apache.oozie.command.coord.CoordActionUpdateXCommand;
import org.apache.oozie.executor.jpa.BulkUpdateInsertJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.service.EventHandlerService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbXOperations;
import org.jdom.Document;
import org.jdom.JDOMException;

@SuppressWarnings("deprecation")
public class ActionStartXCommand extends ActionXCommand<Void> {
    public static final String EL_ERROR = "EL_ERROR";
    public static final String EL_EVAL_ERROR = "EL_EVAL_ERROR";
    public static final String COULD_NOT_START = "COULD_NOT_START";
    public static final String START_DATA_MISSING = "START_DATA_MISSING";
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";

    private List<JsonBean> updateList = new ArrayList<JsonBean>();
    private List<JsonBean> insertList = new ArrayList<JsonBean>();

    public ActionStartXCommand(String actionId, String type) {
        super(actionId, "action.start", type, 0);
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
        if (wfAction.isPending()
                && (wfAction.getStatus() == WorkflowActionBean.Status.PREP
                        || wfAction.getStatus() == WorkflowActionBean.Status.START_RETRY
                        || wfAction.getStatus() == WorkflowActionBean.Status.START_MANUAL
                        || wfAction.getStatus() == WorkflowActionBean.Status.USER_RETRY
                        )) {
            if (wfJob.getStatus() != WorkflowJob.Status.RUNNING) {
                throw new PreconditionException(ErrorCode.E0810, WorkflowJob.Status.RUNNING.toString());
            }
        }
        else {
            throw new PreconditionException(ErrorCode.E0816, wfAction.getPending(), wfAction.getStatusStr());
        }

        loadActionExecutor(true);

    }

    @Override
    protected Void execute() throws CommandException {

        LOG.info("STARTED ActionStartXCommand : status[{0}]", wfAction.getStatus());
        Configuration conf = wfJob.getWorkflowInstance().getConf();

        int maxRetries = 0;
        long retryInterval = 0;

        if (!(executor instanceof ControlNodeActionExecutor)) {
            maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
            retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
        }

        executor.setMaxRetries(maxRetries);
        executor.setRetryInterval(retryInterval);

        ActionExecutorContext context = null;
        try {
            boolean isRetry = false;
            if (wfAction.getStatus() == WorkflowActionBean.Status.START_RETRY
                    || wfAction.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                isRetry = true;
                prepareForRetry(wfAction);
            }
            boolean isUserRetry = false;
            if (wfAction.getStatus() == WorkflowActionBean.Status.USER_RETRY) {
                isUserRetry = true;
                prepareForRetry(wfAction);
            }
            context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
            boolean caught = evaluateActionConf(executor, context);
            if(!caught) {
                execute(executor, context);
                onSuccess(context);
            }
        }
        catch (Throwable t) {
            onFailure(context, t);
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
        }

        LOG.info("ENDED ActionStartXCommand : status[{0}]", wfAction.getStatus());

        return null;
    }

    private boolean execute(ActionExecutor executor, ActionExecutorContext context) throws ActionExecutorException, CommandException, JDOMException {

        LOG.debug("Start, name [{0}] type [{1}] configuration{E}{E}{2}{E}", wfAction.getName(), wfAction.getType(), wfAction.getConf());

        incrActionCounter(wfAction.getType(), 1);

        Instrumentation.Cron cron = new Instrumentation.Cron();
        cron.start();
        context.setStartTime();
        executor.start(context, wfAction);
        cron.stop();
        FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
        addActionCron(wfAction.getType(), cron);

        return true;
    }

    private void onSuccess(ActionExecutorContext context) throws CommandException{
        wfAction.setRetries(0);
        if (wfAction.isExecutionComplete()) {
            if (!context.isExecuted()) {
                LOG.warn(XLog.OPS, "Action Completed, ActionExecutor [{0}] must call setExecutionData()", executor
                        .getType());
                wfAction.setErrorInfo(EXEC_DATA_MISSING,
                        "Execution Complete, but Execution Data Missing from Action");
                failJob(context);
                sendActionNotification();
            } else {
                wfAction.setPending();
                queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
            }
        }
        else {
            if (!context.isStarted()) {
                LOG.warn(XLog.OPS, "Action Started, ActionExecutor [{0}] must call setStartData()", executor
                        .getType());
                wfAction.setErrorInfo(START_DATA_MISSING, "Execution Started, but Start Data Missing from Action");
                failJob(context);
            }
            sendActionNotification();
        }

        LOG.info(XLog.STD, "Action " +
                (wfAction.isExecutionComplete() ? "succeeded" : "started") + ", status=" + wfAction.getStatusStr());


        updateList.add(wfAction);
        wfJob.setLastModifiedTime(new Date());
        updateList.add(wfJob);
        // Add SLA status event (STARTED) for WF_ACTION
        SLAEventBean slaEvent = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(), Status.STARTED,
                SlaAppType.WORKFLOW_ACTION);
        if(slaEvent != null) {
            insertList.add(slaEvent);
        }
        LOG.warn(XLog.STD, "[*** " + wfAction.getId() + " ***]" + "Action updated in DB!");
    }

    protected void onFailure(ActionExecutorContext context, Throwable t) throws CommandException {
        if (t instanceof ActionExecutorException) {
            onActionException(context, (ActionExecutorException) t);
        } else {
            LOG.warn("Unhandled exception", t);
        }
        sendActionNotification();
    }

    private void onActionException(ActionExecutorContext context, ActionExecutorException ex) throws CommandException {
        LOG.warn("Error starting action [{0}]. ErrorType [{1}], ErrorCode [{2}], Message [{3}]",
                wfAction.getName(), ex.getErrorType(), ex.getErrorCode(), ex.getMessage(), ex);
        wfAction.setErrorInfo(ex.getErrorCode(), ex.getMessage());
        switch (ex.getErrorType()) {
            case TRANSIENT:
                handleTransient(context, executor, WorkflowAction.Status.START_RETRY);
                break;
            case NON_TRANSIENT:
                handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                break;
            case ERROR:
                handleError(context, executor, WorkflowAction.Status.ERROR.toString(), WorkflowAction.Status.DONE);
                break;
            case FAILED:
                try {
                    failJob(context);
                    // update coordinator action
                    new CoordActionUpdateXCommand(wfJob, 3).call();
                    new WfEndXCommand(wfJob).call(); // To delete the WF temp dir
                    SLAEventBean slaEvent1 = SLADbXOperations.createStatusEvent(wfAction.getSlaXml(), wfAction.getId(), Status.FAILED,
                            SlaAppType.WORKFLOW_ACTION);
                    if(slaEvent1 != null) {
                        insertList.add(slaEvent1);
                    }
                    SLAEventBean slaEvent2 = SLADbXOperations.createStatusEvent(wfJob.getSlaXml(), wfJob.getId(), Status.FAILED,
                            SlaAppType.WORKFLOW_JOB);
                    if(slaEvent2 != null) {
                        insertList.add(slaEvent2);
                    }
                }
                catch (XException x) {
                    LOG.warn("ActionStartXCommand - case:FAILED ", x.getMessage());
                }
                break;
        }
        updateList.add(wfAction);
        wfJob.setLastModifiedTime(new Date());
        updateList.add(wfJob);
    }

    private boolean evaluateActionConf(ActionExecutor action, ActionExecutorContext context) throws ActionExecutorException, CommandException {
        WorkflowActionBean wfAction = (WorkflowActionBean) context.getAction();

        boolean caught = false;
        try {
            if (!(executor instanceof ControlNodeActionExecutor)) {
                String tmpActionConf = XmlUtils.removeComments(wfAction.getConf());
                String actionConf = context.getELEvaluator().evaluate(tmpActionConf, String.class);
                wfAction.setConf(actionConf);

                //FIXME how to set action XML
                ELEvaluator evaluator = action.preActionEvaluator(context, wfAction);
                Document document = XmlUtils.removeComments(wfAction.getConf(), evaluator);

                if (document != null){
                    context.setActionXML(document.getRootElement());
                }

            }
        }
        catch (ELEvaluationException ex) {
            caught = true;
            sendActionNotification();
            throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, EL_EVAL_ERROR, ex
                    .getMessage(), ex);
        }
        catch (ELException ex) {
            caught = true;
            context.setErrorInfo(EL_ERROR, ex.getMessage());
            LOG.warn("ELException in ActionStartXCommand ", ex.getMessage(), ex);
            handleError(context, wfJob, wfAction);
        }
        catch (org.jdom.JDOMException je) {
            caught = true;
            context.setErrorInfo("ParsingError", je.getMessage());
            LOG.warn("JDOMException in ActionStartXCommand ", je.getMessage(), je);
            handleError(context, wfJob, wfAction);
        }
        catch (Exception ex) {
            caught = true;
            context.setErrorInfo(EL_ERROR, ex.getMessage());
            LOG.warn("Exception in ActionStartXCommand ", ex.getMessage(), ex);
            handleError(context, wfJob, wfAction);
        }

        return caught;
    }

    @Override
    protected boolean handleTransient(ActionExecutor.Context context, ActionExecutor executor,
                                      WorkflowAction.Status status) throws CommandException {
        if (!super.handleTransient(context, executor, WorkflowAction.Status.START_RETRY)) {
            handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
            wfAction.setPendingAge(new Date());
            wfAction.setRetries(0);
            wfAction.setStartTime(null);
            return false;
        }
        return true;
    }

    @Override
    protected boolean handleError(ActionExecutor.Context context, ActionExecutor executor, String message,
                                  WorkflowAction.Status status) throws CommandException {
        if(!super.handleError(context, executor, message, status)) {
            wfAction.setPending();
            wfAction.setExecutionData(message, null);
            queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
            return false;
        }
        return true;
    }

    private void handleError(ActionExecutorContext context, WorkflowJobBean workflow, WorkflowActionBean action)
            throws CommandException {
        sendActionNotification();
        failJob(context);
        updateList.add(wfAction);
        wfJob.setLastModifiedTime(new Date());
        updateList.add(wfJob);
        SLAEventBean slaEvent1 = SLADbXOperations.createStatusEvent(action.getSlaXml(), action.getId(),
                Status.FAILED, SlaAppType.WORKFLOW_ACTION);
        if(slaEvent1 != null) {
            insertList.add(slaEvent1);
        }
        SLAEventBean slaEvent2 = SLADbXOperations.createStatusEvent(workflow.getSlaXml(), workflow.getId(),
                Status.FAILED, SlaAppType.WORKFLOW_JOB);
        if(slaEvent2 != null) {
            insertList.add(slaEvent2);
        }

        new WfEndXCommand(wfJob).call(); //To delete the WF temp dir
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getKey()
     */
    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }

    private void prepareForRetry(WorkflowActionBean wfAction) {
        if (wfAction.getType().equals("map-reduce")) {
            // need to delete child job id of original run
            wfAction.setExternalChildIDs("");
        }
    }

}
