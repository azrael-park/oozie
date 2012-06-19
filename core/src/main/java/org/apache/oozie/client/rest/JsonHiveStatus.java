package org.apache.oozie.client.rest;

import org.apache.oozie.client.HiveStatus;
import org.apache.openjpa.persistence.jdbc.Index;
import org.json.simple.JSONObject;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.DiscriminatorColumn;
import javax.persistence.DiscriminatorType;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.persistence.Transient;
import java.util.Date;

@Entity
@Table(name = "HIVE_STATUS")
@IdClass(JsonHiveStatus.class)
@DiscriminatorColumn(name = "bean_type", discriminatorType = DiscriminatorType.STRING)
public class JsonHiveStatus implements HiveStatus, JsonBean {

    @Id
    @Basic
    @Index
    @Column(name = "wf_id")
    protected String wfId;

    @Id
    @Basic
    @Index
    @Column(name = "action_name", length = 31)
    protected String actionName;

    @Id
    @Basic
    @Index
    @Column(name = "query_id", length = 63)
    protected String queryId;

    @Id
    @Basic
    @Index
    @Column(name = "stage_id", length = 15)
    protected String stageId;

    @Basic
    @Index
    @Column(name = "action_id")
    protected String actionId;

    @Basic
    @Index
    @Column(name = "job_id", length = 31)
    protected String jobId;

    @Basic
    @Index
    @Column(name = "status", length = 15)
    protected String status;

    @Basic
    @Column(name = "failed_tasks", length = 4096)
    protected String failedTasks;

    @Transient
    private Date startTime;

    @Transient
    private Date endTime;

    public String getWfId() {
        return wfId;
    }

    public String getActionId() {
        return actionId;
    }

    public String getActionName() {
        return actionName;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getStageId() {
        return stageId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getStatus() {
        return status;
    }

    public String getFailedTasks() {
        return failedTasks;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.HIVE_STATUS_WF_ID, getWfId());
        json.put(JsonTags.HIVE_STATUS_ACTION_ID, getActionId());
        json.put(JsonTags.HIVE_STATUS_ACTION_NAME, getActionName());
        json.put(JsonTags.HIVE_STATUS_QUERY_ID, getQueryId());
        json.put(JsonTags.HIVE_STATUS_STAGE_ID, getStageId());
        json.put(JsonTags.HIVE_STATUS_JOB_ID, getJobId());
        json.put(JsonTags.HIVE_STATUS_JOB_STATUS, getStatus());
        json.put(JsonTags.HIVE_STATUS_CREATED_TIME, JsonUtils.formatDateRfc822(getStartTime()));
        json.put(JsonTags.HIVE_STATUS_END_TIME, JsonUtils.formatDateRfc822(getEndTime()));
        return json;
    }

    public int hashCode() {
        return wfId.hashCode() + actionName.hashCode();
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof JsonHiveStatus)) {
            return false;
        }
        JsonHiveStatus another = (JsonHiveStatus) obj;
        return wfId.equals(another.wfId) && actionName.equals(another.actionName) && queryId.equals(another.queryId) && stageId.equals(another.stageId);
    }

    public String toString() {
        return getActionId() + "#" + getQueryId() + ":" + getStageId() + " --> " + getJobId() + "[" + getStatus() + "]";
    }
}
