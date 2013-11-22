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
package org.apache.oozie.executor.jpa;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.Query;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.util.ParamChecker;
import org.apache.openjpa.persistence.OpenJPAPersistence;

/**
 * JPA Command to get subset of workflow actions for a particular workflow.
 */
public class WorkflowActionSubsetGetJPAExecutor implements JPAExecutor<List<WorkflowActionBean>> {

    private final String wfId;
    private final int start;
    private final int length;

    /**
     * This Constructor creates the WorkflowActionSubsetGetJPAExecutor object Which gets the List of wrokflow action
     * bean.
     *
     * @param wfId
     * @param start
     * @param length
     */
    public WorkflowActionSubsetGetJPAExecutor(String wfId, int start, int length) {
        ParamChecker.notNull(wfId, "wfJobId");
        this.wfId = wfId;
        this.start = start;
        this.length = length;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {
        try {
            Query q = em.createNamedQuery("GET_ACTIONS_FOR_WORKFLOW");
            OpenJPAPersistence.cast(q);
            q.setParameter("wfId", wfId);
            q.setFirstResult(start - 1);
            q.setMaxResults(length);
            return WorkflowActionBean.duplicate(q.getResultList(), false);
        }
        catch (Exception e) {
            throw new JPAExecutorException(ErrorCode.E0605, "null", e);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "WorkflowActionSubsetGetJPAExecutor";
    }

}
