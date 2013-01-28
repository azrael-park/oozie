package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Query;

import java.util.List;

public interface HiveTClient {

    Query compile(String query) throws Exception;

    void execute() throws Exception;

    void executeTransient(String query) throws Exception;

    List<String> fetchN(int numRows) throws Exception;

    void clear() throws Exception;

    void shutdown() throws Exception;

}
