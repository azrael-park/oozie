package org.apache.oozie.util.db;

import org.apache.oozie.client.OozieClient;

import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PredicateGenerator {

    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;
    private static final long HOUR_IN_MS = 60 * 60 * 1000;
    private static final long MIN_IN_MS = 60 * 1000;
    private static final long SEC_IN_MS = 1000;

    private List<String> colArray = new ArrayList<String>();
    private List<Object> valArray = new ArrayList<Object>();
    private StringBuilder sb = new StringBuilder();

    private transient boolean queryStarted;

    public String[] generate(Map<String, List<String>> filter, String select, String count) {
        int index = 0;
        for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
            if (!entry.getKey().endsWith("Recent")) {
                index = generateQuery(index, entry.getKey(), entry.getValue());
            }
        }
        String orderBy = null;
        for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
            //FIXME external dependency is deprecated.
//            if (entry.getKey().equals(OozieClient.FILTER_STARTED_RECENT)) {
//                generateRecent("startTimestamp", filter.get(OozieClient.FILTER_STARTED_RECENT));
//                orderBy = orderBy == null ? " order by w.startTimestamp desc" : orderBy + ", w.startTimestamp desc";
//            } else if (entry.getKey().equals(OozieClient.FILTER_ENDED_RECENT)) {
//                generateRecent("endTimestamp", filter.get(OozieClient.FILTER_ENDED_RECENT));
//                orderBy = orderBy == null ? " order by w.endTimestamp desc" : orderBy + ", w.endTimestamp desc";
//            }
        }
        count += sb.toString();

        sb.append(orderBy == null ? " order by w.startTimestamp desc" : orderBy);

        select += sb.toString();

        return new String[] {select, count };
    }

    private void generateRecent(String columnName, List<String> expressions) {
        if (expressions == null || expressions.isEmpty()) {
            return;
        }
        if (!queryStarted) {
            sb.append(" where w.").append(columnName).append(" > :").append(columnName);
        } else {
            sb.append(" and w.").append(columnName).append(" > :").append(columnName);
        }
        long recent;
        String expression = expressions.get(0); // use only first one
        if (expression.endsWith("D") || expression.endsWith("d")) {
            recent = Long.valueOf(expression.substring(0, expression.length() - 1)) * DAY_IN_MS;
        } else if (expression.endsWith("H") || expression.endsWith("h")) {
            recent = Long.valueOf(expression.substring(0, expression.length() - 1)) * HOUR_IN_MS;
        } else if (expression.endsWith("M") || expression.endsWith("m")) {
            recent = Long.valueOf(expression.substring(0, expression.length() - 1)) * MIN_IN_MS;
        } else if (expression.endsWith("S") || expression.endsWith("s")) {
            recent = Long.valueOf(expression.substring(0, expression.length() - 1)) * SEC_IN_MS;
        } else {
            recent = Long.valueOf(expression);
        }
        Timestamp from = new Timestamp(System.currentTimeMillis() - recent);
        colArray.add(columnName);
        valArray.add(from);
    }

    private int generateQuery(int index, String colName, List<String> values) {
        if (values.isEmpty()) {
            return index;
        }
        boolean whereStarted = false;
        for (String value : values) {
            String colVar = colName + index;
            if (!queryStarted) {
                sb.append(" where w.").append(colName).append(" IN (:").append(colVar);
                whereStarted = true;
                queryStarted = true;
            } else if (!whereStarted) {
                sb.append(" and w.").append(colName).append(" IN (:").append(colVar);
                whereStarted = true;
            } else {
                sb.append(", :").append(colVar);
            }
            colArray.add(colVar);
            valArray.add(value);
            index++;
        }
        sb.append(")");
        return index;
    }

    public void setParams(Query... queries) {
        for (int i = 0; i < colArray.size(); i++) {
            for (Query query : queries) {
                if (query != null) {
                    query.setParameter(colArray.get(i), valArray.get(i));
                }
            }
        }
    }
}
