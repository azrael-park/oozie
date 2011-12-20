package org.apache.oozie;

import org.apache.oozie.client.rest.JsonHiveStatus;
import org.apache.oozie.util.DateUtils;
import org.apache.openjpa.persistence.jdbc.Index;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Transient;
import java.sql.Timestamp;
import java.util.Date;

@Entity
@NamedQueries({
        @NamedQuery(name = "DELETE_STATUS_WF", query = "delete from HiveQueryStatusBean a where a.wfId = :wfId"),
        @NamedQuery(name = "DELETE_STATUS_WF_ACTION", query = "delete from HiveQueryStatusBean a where a.wfId = :wfId AND a.actionName = :actionName"),

        @NamedQuery(name = "UPDATE_STATUS", query = "update HiveQueryStatusBean a set a.jobId = :jobId, a.status = :status where a.wfId = :wfId AND a.actionName = :actionName AND a.queryId = :queryId AND a.stageId = :stageId"),

        @NamedQuery(name = "GET_STATUS_WF", query = "select OBJECT(a) from HiveQueryStatusBean a where a.wfId = :wfId"),
        @NamedQuery(name = "GET_STATUS_WF_ACTION", query = "select OBJECT(a) from HiveQueryStatusBean a where a.wfId = :wfId AND a.actionName = :actionName"),
        @NamedQuery(name = "GET_STATUS_WF_ACTION_QUERY", query = "select OBJECT(a) from HiveQueryStatusBean a where a.wfId = :wfId AND a.actionName = :actionName AND a.queryId = :queryId"),
        @NamedQuery(name = "GET_STATUS_WF_ACTION_QUERY_STAGE", query = "select OBJECT(a) from HiveQueryStatusBean a where a.wfId = :wfId AND a.actionName = :actionName AND a.queryId = :queryId AND a.stageId = :stageId"),

        @NamedQuery(name = "GET_STATUS_JOB", query = "select OBJECT(a) from HiveQueryStatusBean a where a.jobId = :jobId")
        })
public class HiveQueryStatusBean extends JsonHiveStatus {

    @Transient
    boolean persisted = true;

    @Basic
    @Column(name = "start_time")
    private java.sql.Timestamp startTimestamp = null;

    @Basic
    @Index
    @Column(name = "end_time")
    private java.sql.Timestamp endTimestamp = null;

    public void setWfId(String wfId) {
        this.wfId = wfId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public void setStageId(String stageId) {
        this.stageId = stageId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public boolean isPersisted() {
        return persisted;
    }

    public void setPersisted(boolean persisted) {
        this.persisted = persisted;
    }

    public Timestamp getStartTimestamp() {
        return startTimestamp;
    }

    public Timestamp getEndTimestamp() {
        return endTimestamp;
    }

    @Override
    public Date getStartTime() {
        return DateUtils.toDate(startTimestamp);
    }

    @Override
    public void setStartTime(Date startTime) {
        super.setStartTime(startTime);
        this.startTimestamp = DateUtils.convertDateToTimestamp(startTime);
    }

    @Override
    public Date getEndTime() {
        return DateUtils.toDate(endTimestamp);
    }

    @Override
    public void setEndTime(Date endTime) {
        super.setEndTime(endTime);
        this.endTimestamp = DateUtils.convertDateToTimestamp(endTime);
    }

    public HiveQueryStatusBean clone() {
        HiveQueryStatusBean status = new HiveQueryStatusBean();
        status.setWfId(getWfId());
        status.setActionId(getActionId());
        status.setActionName(getActionName());
        status.setQueryId(getQueryId());
        status.setStageId(getStageId());
        status.setJobId(getJobId());
        status.setStatus(getStatus());
        status.setPersisted(isPersisted());
        status.setStartTime(getStartTime());
        status.setEndTime(getEndTime());
        return status;
    }
}
