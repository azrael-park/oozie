package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.jdbc.Utils;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HiveTClientV2 implements HiveTClient {

    private final Utils.JdbcConnectionParams params;
    private final HiveConnection connection;

    private transient HiveStatement statement;
    private transient ResultSet resultSet;
    private transient ResultSetMetaData schema;

    public HiveTClientV2(HiveConnection connection, Utils.JdbcConnectionParams params) throws Exception {
        this.params = params;
        this.connection = connection;
    }

    @Override
    public Utils.JdbcConnectionParams getConnectionParams() {
        return params;
    }

    @Override
    public Query compile(String query) throws Exception {
        checkStatements();
        statement = (HiveStatement) connection.createStatement();
        if (statement.compile(query)) {
            resultSet = statement.getResultSet();
        }
        return statement.getQueryPlan();
    }

    private void checkStatements() throws SQLException {
        try {
            if (statement != null) {
                statement.close();
            }
        } finally {
            statement = null;
            resultSet = null;
            schema = null;
        }
    }

    @Override
    public void execute() throws Exception {
        if (statement == null) {
            throw new SQLException("Statement is not ready");
        }
        statement.run();
    }

    @Override
    public void executeTransient(String query) throws Exception {
        HiveStatement statement = this.statement;
        if (statement != null) {
            statement.executeTransient(query);
            return;
        }
        statement = (HiveStatement) connection.createStatement();
        try {
            statement.executeTransient(query);
        } finally {
            statement.close();
        }
    }

    @Override
    public List<String> fetchN(int numRows) throws Exception {
        if (resultSet == null) {
            return Collections.emptyList();
        }
        if (schema == null) {
            schema = resultSet.getMetaData();
        }
        List<String> result = new ArrayList<String>(numRows);
        while (resultSet.next()) {
            result.add(toString(resultSet, schema));
        }

        return result;
    }

    @Override
    public void clear() throws Exception {
        checkStatements();
    }

    @Override
    public void shutdown() throws Exception {
        try {
            clear();
        } finally {
            connection.close();
        }
    }

    @Override
    public boolean ping(int timeout) throws Exception {
        connection.setReadTimeout(timeout);
        executeTransient("set ping");
        return true;
    }

    @Override
    public void destroy() {
        try {
            if (connection.isWrapperFor(TSocket.class)) {
                connection.unwrap(TSocket.class).close();
            } else if (connection.isWrapperFor(THttpClient.class)) {
                connection.unwrap(THttpClient.class).close();
            }
        } catch (Exception e) {
            // ignore
        }
    }

    private String toString(ResultSet result, ResultSetMetaData meta) throws Exception {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            if (i > 1) {
                builder.append(" ");
            }
            builder.append(result.getString(i));
        }
        return builder.toString();
    }
}
