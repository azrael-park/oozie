package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.CompileResult;
import org.apache.hive.service.cli.*;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HiveTClientV2 implements HiveTClient {

    private final ThriftCLIServiceClient client;
    private final SessionHandle session;
    private final TTransport transport;

    private transient OperationHandle operation;

    private transient TableSchema schema;
    private transient Iterator<Object[]> iterator;

    public HiveTClientV2(HiveConnection connection) throws Exception {
        this.client = new ThriftCLIServiceClient(connection.getClient());
        this.session = new SessionHandle(connection.getSessHandle());
        this.transport = connection.getTransport();
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
