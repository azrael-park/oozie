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
package org.apache.oozie;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.oozie.client.HiveStatus;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.XConfiguration;

/**
 * Client API to submit and manage Oozie workflow jobs against an Oozie intance. <p/> This class is thread safe. <p/>
 * Syntax for filter for the {@link #getJobsInfo(String)}  {@link #getJobsInfo(String, int, int)}  methods:
 * <code>[NAME=VALUE][;NAME=VALUE]*</code>. <p/> Valid filter names are: <p/> <ul/> <li>name: the workflow application
 * name from the workflow definition.</li> <li>user: the user that submitted the job.</li> <li>group: the group for the
 * job.</li> <li>status: the status of the job.</li> </ul> <p/> The query will do an AND among all the filter names. The
 * query will do an OR among all the filter values for the same name. Multiple values must be specified as different
 * name value pairs.
 */
public class LocalOozieClient extends OozieClient {

    private DagEngine dagEngine;

    /**
     * Create a workflow client for Oozie local use. <p/>
     *
     * @param dagEngine the dag engine instance to use.
     */
    public LocalOozieClient(DagEngine dagEngine) {
        this.dagEngine = dagEngine;
    }

    /**
     * Return the Oozie URL of the workflow client instance. <p/> This URL is the base URL fo the Oozie system, with not
     * protocol versioning.
     *
     * @return the Oozie URL of the workflow client instance.
     */
    @Override
    public String getOozieUrl() {
        return "localoozie";
    }

    /**
     * Return the Oozie URL used by the client and server for WS communications. <p/> This URL is the original URL plus
     * the versioning element path.
     *
     * @return the Oozie URL used by the client and server for communication.
     * @throws org.apache.oozie.client.OozieClientException thrown in the client and the server are not protocol
     * compatible.
     */
    @Override
    public String getProtocolUrl() throws OozieClientException {
        return "localoozie";
    }

    /**
     * Validate that the Oozie client and server instances are protocol compatible.
     *
     * @throws org.apache.oozie.client.OozieClientException thrown in the client and the server are not protocol
     * compatible.
     */
    @Override
    public synchronized void validateWSVersion() throws OozieClientException {
    }

    /**
     * Create an empty configuration with just the {@link #USER_NAME} set to the JVM user name and the {@link
     * #GROUP_NAME} set to 'other'.
     *
     * @return an empty configuration.
     */
    @Override
    public Properties createConfiguration() {
        Properties conf = new Properties();
        if (dagEngine != null) {
            conf.setProperty(USER_NAME, dagEngine.getUser());
        }
        conf.setProperty(GROUP_NAME, "users");
        return conf;
    }

    /**
     * Set a HTTP header to be used in the WS requests by the workflow instance.
     *
     * @param name header name.
     * @param value header value.
     */
    @Override
    public void setHeader(String name, String value) {
    }

    /**
     * Get the value of a set HTTP header from the workflow instance.
     *
     * @param name header name.
     * @return header value, <code>null</code> if not set.
     */
    @Override
    public String getHeader(String name) {
        return null;
    }

    /**
     * Remove a HTTP header from the workflow client instance.
     *
     * @param name header name.
     */
    @Override
    public void removeHeader(String name) {
    }

    /**
     * Return an iterator with all the header names set in the workflow instance.
     *
     * @return header names.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Iterator<String> getHeaderNames() {
        return Collections.EMPTY_SET.iterator();
    }


    /**
     * Submit a workflow job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job could not be submitted.
     */
    @Override
    public String submit(Properties conf) throws OozieClientException {
        try {
            return dagEngine.submitJob(new XConfiguration(conf), false);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Start a workflow job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job could not be started.
     */
    @Override
    public void start(String jobId) throws OozieClientException {
        try {
            dagEngine.start(jobId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Submit and start a workflow job.
     *
     * @param conf job configuration.
     * @return the job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job could not be submitted.
     */
    @Override
    public String run(Properties conf) throws OozieClientException {
        try {
            return dagEngine.submitJob(new XConfiguration(conf), true);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Rerun a workflow job.
     *
     * @param jobId job Id to rerun.
     * @param conf configuration information for the rerun.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job could not be started.
     */
    @Override
    public void reRun(String jobId, Properties conf) throws OozieClientException {
        try {
            dagEngine.reRun(jobId, new XConfiguration(conf));
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Suspend a workflow job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job could not be suspended.
     */
    @Override
    public void suspend(String jobId) throws OozieClientException {
        try {
            dagEngine.suspend(jobId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Resume a workflow job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job could not be resume.
     */
    @Override
    public void resume(String jobId) throws OozieClientException {
        try {
            dagEngine.resume(jobId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * update action attributes.
     *
     * @param id action Id.
     * @throws OozieClientException thrown if the job could not be resume.
     */
    @Override
    public void update(String actionId, Map<String, String> updates) throws OozieClientException {
        try {
            dagEngine.update(actionId, updates);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Kill a workflow job.
     *
     * @param jobId job Id.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job could not be killed.
     */
    @Override
    public void kill(String jobId) throws OozieClientException {
        try {
            dagEngine.kill(jobId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Get the info of a workflow job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job info could not be retrieved.
     */
    @Override
    public WorkflowJob getJobInfo(String jobId) throws OozieClientException {
        try {
            return dagEngine.getJob(jobId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Get the info of a workflow job.
     *
     * @param jobId job Id.
     * @return the job info.
     * @throws org.apache.oozie.client.OozieClientException thrown if the job info could not be retrieved.
     */
    @Override
    public WorkflowAction getWorkflowActionInfo(String actionId) throws OozieClientException {
        try {
            return dagEngine.getWorkflowAction(actionId);
        }
        catch (BaseEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter.
     *
     * @param filter job filter. Refer to the {@link LocalOozieClient} for the filter syntax.
     * @param start jobs offset, base 1.
     * @param len number of jobs to return.
     * @return a list with the workflow jobs info, without node details.
     * @throws org.apache.oozie.client.OozieClientException thrown if the jobs info could not be retrieved.
     */
    @Override
    public List<WorkflowJob> getJobsInfo(String filter, int start, int len) throws OozieClientException {
        try {
            return (List) dagEngine.getJobs(filter, start, len).getWorkflows();
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the info of the workflow jobs that originated from a coordinator job
     *
     * @param coordId originating coordinator id
     * @return a list with the workflow jobs info, without node details.
     * @throws OozieClientException thrown if the jobs info could not be retrieved.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowJob> getJobsForCoord(String coordId) throws OozieClientException {
        try {
            return (List) dagEngine.getJobsForCoord(coordId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the info of the workflow jobs that match the filter. <p/> It returns the first 100 jobs that match the
     * filter.
     *
     * @param filter job filter. Refer to the {@link LocalOozieClient} for the filter syntax.
     * @return a list with the workflow jobs info, without node details.
     * @throws org.apache.oozie.client.OozieClientException thrown if the jobs info could not be retrieved.
     */
    @Override
    public List<WorkflowJob> getJobsInfo(String filter) throws OozieClientException {
        return getJobsInfo(filter, 1, 100);
    }

    /**
     * Return the info of the workflow actions that match the filter.
     *
     * @param filter action filter. Refer to the {@link LocalOozieClient} for the filter syntax.
     * @param start action offset, base 1.
     * @param len number of action to return.
     * @return a list of the workflow action info, without node details.
     * @throws org.apache.oozie.client.OozieClientException thrown if the action info could not be retrieved.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<WorkflowAction> getActionsInfo(String filter, int start, int len) throws OozieClientException {
        try {
            return (List) dagEngine.getActions(filter, start, len).getActions();
        } catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    /**
     * Return the workflow job Id for an external Id. <p/> The external Id must have provided at job creation time.
     *
     * @param externalId external Id given at job creation time.
     * @return the workflow job Id for an external Id, <code>null</code> if none.
     * @throws org.apache.oozie.client.OozieClientException thrown if the operation could not be done.
     */
    @Override
    public String getJobId(String externalId) throws OozieClientException {
        try {
            return dagEngine.getJobIdForExternalId(externalId);
        }
        catch (DagEngineException ex) {
            throw new OozieClientException(ex.getErrorCode().toString(), ex);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<HiveStatus> getHiveStatusListForQueryID(String actionID, String queryID) throws OozieClientException {
        try {
            return (List)dagEngine.getHiveStatusListForQueryID(actionID, queryID);
        } catch (XException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<HiveStatus> getHiveStatusListForActionID(String actionID) throws OozieClientException {
        try {
            return (List)dagEngine.getHiveStatusListForActionID(actionID);
        } catch (XException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<HiveStatus> getHiveStatusListForWorkflowID(String wfID) throws OozieClientException {
        try {
            return (List)dagEngine.getHiveStatusListForWorkflowID(wfID);
        } catch (XException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public HiveStatus getHiveStatusForStageID(String actionID, String queryID, String stageID) throws OozieClientException {
        try {
            return dagEngine.getHiveStatusForStageID(actionID, queryID, stageID);
        } catch (XException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public HiveStatus getHiveStatusForJobID(String jobID) throws OozieClientException {
        try {
            return dagEngine.getHiveStatusForJobID(jobID);
        } catch (XException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    @Override
    public Map<String, List<String>> getFailedTaskURLs(String id) throws OozieClientException {
        try {
            return dagEngine.getFailedTaskURLs(id);
        } catch (XException e) {
            throw new OozieClientException(e.getErrorCode().toString(), e);
        }
    }

    /**
     * Returns if Oozie is in safe mode or not.
     *
     * @return true if safe mode is ON<br> false if safe mode is OFF
     * @throws org.apache.oozie.client.OozieClientException throw if it could not obtain the safe mode status.
     */
    /*public SYSTEM_MODE isInSafeMode() throws OozieClientException {
        //return Services.get().isSafeMode();
        return Services.get().getSystemMode() ;
    }*/

}
