package org.apache.oozie.executor.jpa;

import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.util.ParamChecker;

import javax.persistence.EntityManager;

public class HiveStatusInsertJPAExecutor implements JPAExecutor<Void> {

    private HiveQueryStatusBean hiveAction;

    public HiveStatusInsertJPAExecutor(HiveQueryStatusBean hiveAction) {
        this.hiveAction = ParamChecker.notNull(hiveAction, "hiveAction");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#getName()
     */
    @Override
    public String getName() {
        return "HiveStatusInsertJPAExecutor";
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.executor.jpa.JPAExecutor#execute(javax.persistence.EntityManager)
     */
    @Override
    public Void execute(EntityManager em) throws JPAExecutorException {
        em.merge(hiveAction);
        return null;
    }
}