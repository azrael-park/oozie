package org.apache.oozie.client;

import java.util.Date;

public interface HiveStatus {

    public static enum Status {
        NOT_STARTED, SUCCEEDED, KILLED, FAILED, FAILED_KILLED, RUNNING
    }

    String getWfId();

    String getActionId();

    String getActionName();

    String getQueryId();

    String getStageId();

    String getJobId();

    String getStatus();

    String getFailedTasks();

    Date getStartTime();

    Date getEndTime();
}
