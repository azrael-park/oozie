package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.CompileResult;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HiveTClientV2 implements HiveTClient {

    private final Utils.JdbcConnectionParams params;
    private final HiveConnection connection;
    private final ICLIService client;
    private final SessionHandle session;
    private final TTransport transport;

    private transient OperationHandle operation;

    private transient TableSchema schema;
    private transient Iterator<Object[]> iterator;

    public HiveTClientV2(HiveConnection connection, Utils.JdbcConnectionParams params) throws Exception {
        this.params = params;
        this.connection = connection;
        this.client = new ThriftCLIServiceClient(connection.getClient()) {

            @Override
            public void runStatement(SessionHandle session, OperationHandle operation) throws HiveSQLException {
                super.runStatement(session, operation);
            }

            @Override
            public void closeSession(SessionHandle session) throws HiveSQLException {
                setTimeout(HiveSession.DEFAULT_TIMEOUT);
                try {
                    super.closeSession(session);
                } finally {
                    setTimeout(0);
                }
            }

            @Override
            public void closeOperation(OperationHandle operation) throws HiveSQLException {
                setTimeout(HiveSession.DEFAULT_TIMEOUT);
                try {
                    super.closeOperation(operation);
                } finally {
                    setTimeout(0);
                }
            }
        };
        this.session = new SessionHandle(connection.getSessHandle());
        this.transport = connection.getTransport();
    }

    private void setTimeout(int timeout) {
        TSocket socket = getSocket(transport, connection);
        if (socket != null) {
            socket.setTimeout(timeout);
        }
    }

    private TSocket getSocket(TTransport transport, HiveConnection connection) {
        try {
            if (connection != null && connection.isWrapperFor(TSocket.class)) {
                return connection.unwrap(TSocket.class);
            }
        } catch (Exception e) {
            // ignore
        }
        while (true) {
            if (transport instanceof TSocket) {
                return (TSocket) transport;
            }
            if (transport instanceof TSaslClientTransport) {
                transport = ((TSaslClientTransport) transport).getUnderlyingTransport();
            } else {
                return null;
            }
        }
    }

    @Override
    public Utils.JdbcConnectionParams getConnectionParams() {
        return params;
    }

    @Override
    public Query compile(String query) throws Exception {
        CompileResult result = client.compileStatement(session, query, null);
        if (result.getHandle() != null) {
            operation = new OperationHandle(result.getHandle());
        }
        return result.getPlan();
    }

    @Override
    public void execute() throws Exception {
        assert operation != null;
        client.runStatement(session, operation);
    }

    @Override
    public void executeTransient(String query) throws Exception {
        client.executeTransient(session, query, null);
    }

    @Override
    public List<String> fetchN(int numRows) throws Exception {
        if (schema == null) {
            schema = client.getResultSetMetadata(operation);
        }
        List<String> result = new ArrayList<String>();
        while (result.size() < numRows) {
            if (iterator == null || !iterator.hasNext()) {
                RowSet rows = client.fetchResults(operation, FetchOrientation.FETCH_NEXT, numRows - result.size());
                iterator = rows.iterator();
            }
            if (!iterator.hasNext()) {
                break;
            }
            result.add(toString(iterator.next()));
        }

        return result;
    }

    @Override
    public void clear() throws Exception {
        if (operation != null) {
            client.closeOperation(operation);
            operation = null;
            iterator = null;
            schema = null;
        }
    }

    @Override
    public void shutdown() throws Exception {
        try {
            clear();
            client.closeSession(session);
        } finally {
            transport.close();
        }
    }

    @Override
    public boolean ping(int timeout) throws HiveSQLException {
        setTimeout(timeout);
        OperationHandle current = operation;
        if (current == null) {
            client.executeTransient(session, "set ping", null);
            return true;
        }
        OperationState status = client.getOperationStatus(current);
        if (status == OperationState.INITIALIZED || status == OperationState.RUNNING) {
            return false;
        }
        if (status == OperationState.FINISHED) {
            return true;
        }
        throw new HiveSQLException("Operation failed with status " + status);
    }

    @Override
    public void destroy() {
        try {
            getSocket(transport, connection).close();
        } catch (Exception e) {
            // ignore
        }
    }

    private String toString(Object[] row) throws Exception {
        List<ColumnDescriptor> columns = schema.getColumnDescriptors();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            Object eval = RowSet.evaluate(columns.get(i), row[i]);
            if (builder.length() > 0) {
                builder.append(" ");
            }
            builder.append(eval);
        }
        return builder.toString();
    }
}
