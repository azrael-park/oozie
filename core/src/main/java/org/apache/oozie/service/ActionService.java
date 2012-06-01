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
package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.control.EndActionExecutor;
import org.apache.oozie.action.control.ForkActionExecutor;
import org.apache.oozie.action.control.JoinActionExecutor;
import org.apache.oozie.action.control.KillActionExecutor;
import org.apache.oozie.action.control.StartActionExecutor;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.Service;
import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.ErrorCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ActionService implements Service {

    public static final String CONF_ACTION_EXECUTOR_CLASSES = CONF_PREFIX + "ActionService.executor.classes";

    public static final String CONF_ACTION_EXECUTOR_EXT_CLASSES = CONF_PREFIX + "ActionService.executor.ext.classes";

    public static final String CONF_ACTION_EXCEPTION_OVERRIDE = CONF_PREFIX + "ActionService.exception.override";

    private static final Pattern OVERRIDE_PATTERN = Pattern.compile(
            "(.+)=(.+)\\[(TRANSIENT|NON_TRANSIENT|ERROR|FAILED|\\?)\\|(.+)\\]");

    private Services services;
    private List<Overriding> overridings;
    private Map<String, Class<? extends ActionExecutor>> executors;

    private final XLog log = XLog.getLog(getClass());

    @SuppressWarnings("unchecked")
    public void init(Services services) throws ServiceException {
        this.services = services;
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();

        overridings = new ArrayList<Overriding>();
        executors = new HashMap<String, Class<? extends ActionExecutor>>();

        Configuration conf = services.getConf();
        for (String override : conf.getStrings(CONF_ACTION_EXCEPTION_OVERRIDE)) {
            Matcher matcher = OVERRIDE_PATTERN.matcher(override);
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid override pattern string " + override);
            }
            log.warn("Exception handling will be overrode by " + override);
            overridings.add(new Overriding(override, matcher));
        }

        Class<? extends ActionExecutor>[] classes = new Class[] { StartActionExecutor.class,
                EndActionExecutor.class, KillActionExecutor.class,  ForkActionExecutor.class, JoinActionExecutor.class };
        registerExecutors(classes);

        classes = (Class<? extends ActionExecutor>[]) services.getConf().getClasses(CONF_ACTION_EXECUTOR_CLASSES);
        registerExecutors(classes);

        classes = (Class<? extends ActionExecutor>[]) services.getConf().getClasses(CONF_ACTION_EXECUTOR_EXT_CLASSES);
        registerExecutors(classes);
    }

    private void registerExecutors(Class<? extends ActionExecutor>[] classes) throws ServiceException {
        if (classes != null) {
            for (Class<? extends ActionExecutor> executorClass : classes) {
                register(executorClass);
            }
        }
    }

    public void destroy() {
        ActionExecutor.enableInit();
        ActionExecutor.resetInitInfo();
        ActionExecutor.disableInit();
        executors = null;
    }

    public Class<? extends Service> getInterface() {
        return ActionService.class;
    }

    public void register(Class<? extends ActionExecutor> klass) throws ServiceException {
        XLog log = XLog.getLog(getClass());
        ActionExecutor executor = (ActionExecutor) ReflectionUtils.newInstance(klass, services.getConf());
        log.trace("Registering action type [{0}] class [{1}]", executor.getType(), klass);
        if (executors.containsKey(executor.getType())) {
            throw new ServiceException(ErrorCode.E0150, executor.getType());
        }
        ActionExecutor.enableInit();
        executor.initActionType();
        for (Overriding overriding : overridings) {
            overriding.override(klass, executor);
        }
        ActionExecutor.disableInit();
        executors.put(executor.getType(), klass);
        log.trace("Registered Action executor for action type [{0}] class [{1}]", executor.getType(), klass);
    }

    public ActionExecutor getExecutor(String actionType) {
        ParamChecker.notEmpty(actionType, "actionType");
        Class<? extends ActionExecutor> executorClass = executors.get(actionType);
        return (executorClass != null) ? (ActionExecutor) ReflectionUtils.newInstance(executorClass, null) : null;
    }

    public ActionExecutor getExecutor(String actionType, Configuration conf) {
        ActionExecutor executor = getExecutor(actionType);
        if (executor != null) {
            int maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
            long retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
            executor.setMaxRetries(maxRetries);
            executor.setRetryInterval(retryInterval);
        }
        return executor;
    }

    private class Overriding {
        Pattern classPattern;
        String exception;
        ActionExecutorException.ErrorType type;
        String code;
        String source;
        Overriding(String source, Matcher matcher) {
            this.source = source.substring(matcher.start(2));
            classPattern = Pattern.compile(convertToGlob(matcher.group(1)));
            exception = matcher.group(2);
            type = matcher.group(3).equals("?") ? null : ActionExecutorException.ErrorType.valueOf(matcher.group(3));
            code = matcher.group(4).equals("?") ? null : matcher.group(4);
        }
        void override(Class clazz, ActionExecutor executor) {
            if (classPattern.matcher(clazz.getName()).matches()) {
                log.info("Exception handling for action type '" + executor.getType() +
                        "' will be overrode by " + source);
                executor.registerOverride(exception, type, code);
            }
        }
    }

    private String convertToGlob(String pattern) {
        boolean escape = false;
        StringBuilder builder = new StringBuilder();
        for (char achar : pattern.toCharArray()) {
            if (escape) {
                builder.append(achar);
                escape = false;
            } else if (achar == '\\') {
                escape = true;
            } else if (achar == '*') {
                builder.append(".+");
            } else if (achar == '?') {
                builder.append('.');
            } else {
                builder.append(achar);
            }
        }
        return builder.toString();
    }
}
