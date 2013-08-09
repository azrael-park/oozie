package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.QueryPlan;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hive.jdbc.Utils;
import org.apache.thrift.transport.TSocket;

import java.util.List;

public class HiveTClientV1 implements HiveTClient {

    final TSocket socket;
    final ThriftHive.Client client;
    final Utils.JdbcConnectionParams params;

    public HiveTClientV1(TSocket socket, ThriftHive.Client client, Utils.JdbcConnectionParams params) {
        this.params = params;
        this.socket = socket;
        this.client = client;
    }

    @Override
    public Utils.JdbcConnectionParams getConnectionParams() {
        return params;
    }

    @Override
    public Query compile(String query) throws Exception {
        QueryPlan plan = client.compile(query);
        assert plan.getQueriesSize() <= 1;
        return plan.getQueriesSize() == 0 ? null : plan.getQueries().get(0);
    }

    @Override
    public void execute() throws Exception {
        client.run();
    }

    @Override
    public void executeTransient(String query) throws Exception {
        client.executeTransient(query);
    }

    @Override
    public void clear() throws Exception {
        client.clean();
    }

    @Override
    public List<String> fetchN(int numRows) throws Exception {
        return client.fetchN(numRows);
    }

    @Override
    public void shutdown(boolean interanl) throws Exception {
        client.shutdown();
    }

    @Override
    public boolean ping(int timeout) {
        return true;
    }

    @Override
    public void destroy() {
        try {
            socket.close();
        } catch (Exception e) {
            // ignore
        }
    }
}
