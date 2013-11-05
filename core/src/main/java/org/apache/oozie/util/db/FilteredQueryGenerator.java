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

    private Map<String, List<String>> filter;
    private int start;
    private int len;

    public FilteredQueryGenerator(Map<String, List<String>> filter) {
        this(filter, 1, 1);
    }

    public FilteredQueryGenerator(Map<String, List<String>> filter, int start, int len) {
        this.filter = filter;
        this.start = start;
        this.len = len;
    }

    public Query[] generate(EntityManager em, String select, String count, int fetchSize) {

        PredicateGenerator generator = new PredicateGenerator();
        String[] queries = generator.generate(filter, select, count);

        Query q = em.createQuery(queries[0]);
        q.setFirstResult(start - 1);
        q.setMaxResults(len);
        Query qTotal = em.createQuery(queries[1]);

        generator.setParams(q, qTotal);

        OpenJPAQuery kq = OpenJPAPersistence.cast(q);
        JDBCFetchPlan fetch = (JDBCFetchPlan) kq.getFetchPlan();
        fetch.setFetchBatchSize(fetchSize);
        fetch.setResultSetType(ResultSetType.SCROLL_INSENSITIVE);
        fetch.setFetchDirection(FetchDirection.FORWARD);
        fetch.setLRSSizeAlgorithm(LRSSizeAlgorithm.LAST);

        return new Query[]{q, qTotal};
    }
}
