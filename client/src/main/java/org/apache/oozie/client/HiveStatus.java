package org.apache.oozie.client;

public interface HiveStatus {

    public static enum Status {
        NOT_STARTED, SUCCEEDED, KILLED, FAILED, FAILED_KILLED, RUNNING
    }

    String getWfId();

    String getActionName();

    String getQueryId();

    String getStageId();

    String getJobId();

    String getStatus();
}
