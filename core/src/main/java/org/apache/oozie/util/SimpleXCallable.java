package org.apache.oozie.util;

import java.util.UUID;
import java.util.concurrent.Callable;

public class SimpleXCallable<T> implements XCallable<T> {

    private long createdTime;
    private Callable<T> callable;
    private String key;
    private String type;


    public SimpleXCallable(Callable<T> callable, String type) {
        this.callable = callable;
        this.createdTime = System.currentTimeMillis();
        this.key = getName() + "_" + UUID.randomUUID();
        this.type = type;
    }

    public String getName() {
        return callable.getClass().getSimpleName();
    }

    public int getPriority() {
        return 0;
    }

    public String getType() {
        return type;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String getEntityKey() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setInterruptMode(boolean mode) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean inInterruptMode() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public T call() throws Exception {
        return callable.call();
    }
}
