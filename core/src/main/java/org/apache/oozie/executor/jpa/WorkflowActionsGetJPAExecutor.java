package org.apache.oozie.executor.jpa;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.db.PredicateGenerator;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WorkflowActionsGetJPAExecutor implements JPAExecutor<List<WorkflowActionBean>> {

    private static final String selectStr = "select w.id, w.name, w.status, w.type, w.startTimestamp, w.endTimestamp, " +
            "w.lamaUserId, w.lamaWorkingSet, w.lamaProjectId, w.lamaApplicationId from WorkflowActionBean w";
    private static final String countStr = "select count(w) from WorkflowActionBean w";

    private final Map<String, List<String>> filter;
    private final int start;
    private final int len;

    public WorkflowActionsGetJPAExecutor(Map<String, List<String>> filter) {
        this(filter, 1, 1);
    }

    public WorkflowActionsGetJPAExecutor(Map<String, List<String>> filter, int start, int len) {
        this.filter = filter;
        this.start = start;
        this.len = len;
    }

    @SuppressWarnings("unchecked")
    public List<WorkflowActionBean> execute(EntityManager em) throws JPAExecutorException {

        PredicateGenerator generator = new PredicateGenerator();
        String[] queries = generator.generate(filter, selectStr, countStr);
        Query q = em.createQuery(queries[0]);
        q.setFirstResult(start - 1);
        q.setMaxResults(len);
        generator.setParams(q);

        List<Object[]> results = (List<Object[]>) q.getResultList();
        List<WorkflowActionBean> actions = new ArrayList<WorkflowActionBean>();

        for (Object[] arr : results) {
            actions.add(getBeanForActionFromArray(arr));
        }
        return actions;
    }

    private WorkflowActionBean getBeanForActionFromArray(Object[] arr) {
        WorkflowActionBean action = new WorkflowActionBean();
        action.setId((String) arr[0]);
        if (arr[1] != null) {
            action.setName((String) arr[1]);
        }
        if (arr[2] != null) {
            action.setStatus(WorkflowAction.Status.valueOf((String) arr[2]));
        }
        if (arr[3] != null) {
            action.setType((String) arr[3]);
        }
        if (arr[4] != null) {
            action.setStartTime((Timestamp) arr[4]);
        }
        if (arr[5] != null) {
            action.setEndTime((Timestamp) arr[5]);
        }

        return action;
    }

    public String getName() {
        return "WorkflowActionsGetJPAExecutor";
    }
}
