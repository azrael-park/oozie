package org.apache.oozie.util.db;

import org.apache.openjpa.persistence.OpenJPAPersistence;
import org.apache.openjpa.persistence.OpenJPAQuery;
import org.apache.openjpa.persistence.jdbc.FetchDirection;
import org.apache.openjpa.persistence.jdbc.JDBCFetchPlan;
import org.apache.openjpa.persistence.jdbc.LRSSizeAlgorithm;
import org.apache.openjpa.persistence.jdbc.ResultSetType;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.util.List;
import java.util.Map;

public class FilteredQueryGenerator {

    private final String select;
    private final String count;
    private final String selectAll;
    private final String countAll;
    private final int fetchSize;

    public FilteredQueryGenerator(String select, String count, String selectAll, String countAll, int fetchSize) {
        this.select = select;
        this.count = count;
        this.selectAll = selectAll;
        this.countAll = countAll;
        this.fetchSize = fetchSize;
    }

    public Query[] generate(EntityManager em, Map<String, List<String>> filter, int start, int length, boolean sizeOnly) {

        Query[] queries = new Query[2];
        if (filter == null || filter.isEmpty()) {
            if (!sizeOnly) {
                queries[0] = em.createNamedQuery(selectAll);
            }
            queries[1] = em.createNamedQuery(countAll);
            return paging(queries, start, length);
        }

        PredicateGenerator generator = new PredicateGenerator();
        String[] generated = generator.generate(filter, select, count);

        if (!sizeOnly) {
            queries[0] = em.createQuery(generated[0]);
            OpenJPAQuery kq = OpenJPAPersistence.cast(queries[0]);
            JDBCFetchPlan fetch = (JDBCFetchPlan) kq.getFetchPlan();
            fetch.setFetchBatchSize(fetchSize);
            fetch.setResultSetType(ResultSetType.SCROLL_INSENSITIVE);
            fetch.setFetchDirection(FetchDirection.FORWARD);
            fetch.setLRSSizeAlgorithm(LRSSizeAlgorithm.LAST);
        }
        queries[1] = em.createQuery(generated[1]);

        generator.setParams(queries);

        return paging(queries, start, length);
    }

    private Query[] paging(Query[] result, int start, int length) {
        if (result[0] != null && start > 0) {
            result[0].setFirstResult(start - 1);
            result[0].setMaxResults(length);
        }
        return result;
    }
}
