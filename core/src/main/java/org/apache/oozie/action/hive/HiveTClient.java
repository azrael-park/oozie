package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hive.jdbc.Utils;

import java.util.List;

public interface HiveTClient {

    Utils.JdbcConnectionParams getConnectionParams();

    Query compile(String query) throws Exception;

    void execute() throws Exception;

    void executeTransient(String query) throws Exception;

    List<String> fetchN(int numRows) throws Exception;

    void clear() throws Exception;

    void shutdown() throws Exception;

    void destroy();

    boolean ping(int timeout) throws Exception;
}
