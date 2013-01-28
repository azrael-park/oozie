package org.apache.oozie.action.hive;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.QueryPlan;
import org.apache.hadoop.hive.service.ThriftHive;

import java.util.List;

public class HiveTClientV1 implements HiveTClient {

    final ThriftHive.Client client;

    public HiveTClientV1(ThriftHive.Client client) {
        this.client = client;
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
    public void shutdown() throws Exception {
        client.shutdown();
    }
}
