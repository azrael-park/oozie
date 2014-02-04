package org.apache.oozie.executor.jpa;

import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowActionInfo;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.util.db.FilteredQueryGenerator;
import org.apache.oozie.util.db.PredicateGenerator;

import javax.persistence.EntityManager;
import javax.persistence.Query;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class WorkflowActionsGetJPAExecutor implements JPAExecutor<WorkflowActionInfo> {

    private static final String SELECT = "select w.id, w.name, w.status, w.type, w.startTimestamp, w.endTimestamp, w.conf " +
            "from WorkflowActionBean w";
    private static final String COUNT = "select count(w) from WorkflowActionBean w";
    private static final int DEFAULT_FETCH = 20;

    private static final FilteredQueryGenerator GENERATOR =
            new FilteredQueryGenerator(SELECT, COUNT, "GET_ACTIONS_COLUMNS", "GET_ACTIONS_COUNT", DEFAULT_FETCH);

    private final Map<String, List<String>> filter;
    private int start;
    private int len;
    private boolean sizeOnly;

    public WorkflowActionsGetJPAExecutor(Map<String, List<String>> filter, boolean sizeOnly) {
        this.filter = filter;
        this.sizeOnly = sizeOnly;
        if(sizeOnly){
            this.start = -1;
            this.len = -1;
        }
    }

    public WorkflowActionsGetJPAExecutor(Map<String, List<String>> filter, int start, int len) {
        this.filter = filter;
        this.start = start;
        this.len = len;
    }

    @SuppressWarnings("unchecked")
    public WorkflowActionInfo execute(EntityManager em) throws JPAExecutorException {

        Query[] result = GENERATOR.generate(em, filter, start, len, sizeOnly);
        Query query = result[0];
        Query count = result[1];

        List<WorkflowActionBean> actions = Collections.emptyList();
        if (query != null) {
            actions = getBeansForActionFromArray(query.getResultList());
        }
        int realLen = ((Long) count.getSingleResult()).intValue();
        return new WorkflowActionInfo(actions, start, len, realLen);
    }

    private List<WorkflowActionBean> getBeansForActionFromArray(List<Object[]> arrs) {
        List<WorkflowActionBean> actions = new ArrayList<WorkflowActionBean>();
        for (Object arr : arrs) {
            actions.add(getBeanForActionFromArray((Object[]) arr));
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
        if (arr[6] != null) {
            action.setConf((String) arr[6]);
        }
        
        return action;
    }

    public String getName() {
        return "WorkflowActionsGetJPAExecutor";
    }
}
