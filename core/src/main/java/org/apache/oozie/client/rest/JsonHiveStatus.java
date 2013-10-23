package org.apache.oozie.client.rest;

import org.apache.oozie.client.HiveStatus;
import org.apache.openjpa.persistence.jdbc.Index;
import org.json.simple.JSONObject;

import javax.persistence.*;

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
    @Column(name = "action_name")
    protected String actionName;

    @Id
    @Basic
    @Index
    @Column(name = "query_Id")
    protected String queryId;

    @Id
    @Basic
    @Index
    @Column(name = "stage_Id")
    protected String stageId;

    @Basic
    @Index
    @Column(name = "job_Id")
    protected String jobId;

    @Basic
    @Index
    @Column(name = "status")
    protected String status;

    public String getWfId() {
        return wfId;
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

    @SuppressWarnings("unchecked")
    public JSONObject toJSONObject() {
        return toJSONObject("GMT");
    }

    @Override
    public JSONObject toJSONObject(String timeZoneId) {
        JSONObject json = new JSONObject();
        json.put(JsonTags.HIVE_STATUS_WF_ID, getWfId());
        json.put(JsonTags.HIVE_STATUS_ACTION_NAME, getActionName());
        json.put(JsonTags.HIVE_STATUS_QUERY_ID, getQueryId());
        json.put(JsonTags.HIVE_STATUS_STAGE_ID, getStageId());
        json.put(JsonTags.HIVE_STATUS_JOB_ID, getJobId());
        json.put(JsonTags.HIVE_STATUS_JOB_STATUS, getStatus());
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
        return getWfId() + "@" + getActionName() + "#" + getQueryId() + ":" + getStageId() + " --> " + getJobId() + "[" + getStatus() + "]";
    }
}
