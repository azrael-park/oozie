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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.WorkflowJobsForCoordJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;

import java.util.List;

public class JobsForCoordXCommand extends WorkflowXCommand<List<WorkflowJobBean>> {

    private final String coordId;

    public JobsForCoordXCommand(String coordId) {
        super("job.info", "job.info", 1, true);
        this.coordId = coordId;
    }

    protected List<WorkflowJobBean> execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService == null) {
                throw new CommandException(ErrorCode.E0610);
            }
            List<WorkflowJobBean> jobs = jpaService.execute(new WorkflowJobsForCoordJPAExecutor(coordId));
            for (WorkflowJobBean workflow : jobs) {
                workflow.setConsoleUrl(JobXCommand.getJobConsoleUrl(workflow.getId()));
            }
            return jobs;
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex);
        }
    }

    public String getEntityKey() {
        return null;
    }

    protected boolean isLockRequired() {
        return false;
    }

    protected void loadState() throws CommandException {
    }

    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}
