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
package org.apache.oozie.action.decision;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import javax.servlet.jsp.el.ELException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.oozie.action.ActionExecutorException.ErrorType.NON_TRANSIENT;

public class DecisionActionExecutor extends ActionExecutor {
    public static final String ACTION_TYPE = "switch";

    private static final String TRUE = "true";

    public DecisionActionExecutor() {
        super(ACTION_TYPE);
    }

    @Override
    public void initActionType() {
        super.initActionType();
        registerError(JDOMException.class.getName(), NON_TRANSIENT, XML_ERROR);
        registerError(ELException.class.getName(), NON_TRANSIENT, EL_ERROR);
        //FIXME : mortbay.jetty.jsp
        //registerError(javax.el.ELException.class.getName(), NON_TRANSIENT, EL_ERROR);
    }

    @Override
    public boolean suspendJobForFail(WorkflowAction.Status status) {
        return false;
    }

    @Override
    public ELEvaluator preActionEvaluator(Context context, WorkflowAction action) {
        return null;
    }

    @Override
    public void updateAttributes(WorkflowActionBean wfAction, Map<String, String> updates) throws Exception {
        String transition = updates.remove("default");
        if (transition == null || transition.isEmpty()) {
            throw new CommandException(ErrorCode.E0830, wfAction.getType(), "default");
        }
        Element config = XmlUtils.parseXml(wfAction.getConf());
        config.getChild("default").setAttribute("to", transition, config.getNamespace());

        config.removeChildren("case");
        for (Map.Entry<String, String> entry : updates.entrySet()) {
            Element element = new Element("case", config.getNamespace());
            element.setAttribute("to", entry.getKey(), config.getNamespace()).setText(entry.getValue());
            config.addContent(element);
        }
        wfAction.setConf(XmlUtils.prettyPrint(config).toString());
    }

    @SuppressWarnings("unchecked")
    public void start(Context context, WorkflowAction action) throws ActionExecutorException {
        XLog log = XLog.getLog(getClass());
        log.trace("start() begins");
        try {
            String confStr = action.getConf();
            context.setStartData("-", "-", "-");
            Element conf = XmlUtils.parseXml(confStr);
            Namespace ns = conf.getNamespace();

            String externalState = null;

            for (Element eval : (List<Element>) conf.getChildren("case", ns)) {
                if (TRUE.equals(eval.getTextTrim())) {
                    externalState = eval.getAttributeValue("to");
                    break;
                }
            }
            if (externalState == null) {
                Element def = conf.getChild("default", ns);
                if (def != null) {
                    externalState = def.getAttributeValue("to");
                }
            }

            if (externalState == null) {
                throw new IllegalStateException("Transition cannot be NULL");
            }
            // for decision we are piggybacking on external status to transfer the transition,
            // the {@link ActionEndCommand} does the special handling of setting it as signal value.
            context.setExecutionData(externalState, null);
        }
        catch (Throwable ex) {
            throw convertException(ex);
        }
        finally {
            log.trace("start() ends");
        }
    }

    public void end(Context context, WorkflowAction action) throws ActionExecutorException {
        context.setEndData(WorkflowAction.Status.OK, action.getExternalStatus());
    }

    public void check(Context context, WorkflowAction action) throws ActionExecutorException {
        throw new UnsupportedOperationException();
    }

    public void kill(Context context, WorkflowAction action) throws ActionExecutorException {
        throw new UnsupportedOperationException();
    }

    public boolean isCompleted(String actionID, String externalStatus, Properties actionData) {
        return true;
    }

}
