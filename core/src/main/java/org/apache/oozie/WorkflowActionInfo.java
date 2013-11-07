package org.apache.oozie;

import java.util.List;

public class WorkflowActionInfo {

    private int start;
    private int len;
    private int total;
    private List<WorkflowActionBean> actions;

    /**
     * Create a coordiantor actions info bean.
     *
     * @param coordiantor actions being returned.
     */
    public WorkflowActionInfo(List<WorkflowActionBean> actions) {
        this.actions = actions;
        this.start = 1;
        this.len = actions.size();
        this.total = actions.size();
    }

    public WorkflowActionInfo(List<WorkflowActionBean> actions, int start, int len, int total) {
        this.actions = actions;
        this.start = start;
        this.len = len;
        this.total = total;
    }

    /**
     * Return the workflow actions being returned.
     *
     * @return the workflow actions being returned.
     */
    public List<WorkflowActionBean> getActions() {
        return actions;
    }

    /**
     * Return the offset of the workflows being returned. <p/> For pagination purposes.
     *
     * @return the offset of the workflows being returned.
     */
    public int getStart() {
        return start;
    }

    /**
     * Return the number of the workflows being returned. <p/> For pagination purposes.
     *
     * @return the number of the workflows being returned.
     */
    public int getLen() {
        return len;
    }

    /**
     * Return the total number of workflows. <p/> For pagination purposes.
     *
     * @return the total number of workflows.
     */
    public int getTotal() {
        return total;
    }
}

