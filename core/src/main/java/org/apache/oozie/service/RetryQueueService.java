package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class RetryQueueService implements Service {
    
    public XLog LOG = XLog.getLog(getClass());
    
    private boolean workerExecutor;
    private Worker w;
    
    public static final String CONF_PREFIX = Service.CONF_PREFIX + "RetryQueueService.";
    
    public static final String CONF_RETRY_ENABLED = CONF_PREFIX + "enabled";
    public static final String CONF_RETRY_EXECUTOR = CONF_PREFIX + "executor";
    
    @Override
    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();
        
        // worker | threadpool
        workerExecutor = conf.get(CONF_RETRY_EXECUTOR, "worker").equals("worker");
        if (workerExecutor) {
            createThread();
        }
    }
    
    @Override
    public void destroy() {
        try {
            if (w != null) {
                w.interruptNow();
            }
            retryQueue.clear();
        } catch (Exception e) {
            LOG.warn(e);
        }
    }
    
    @Override
    public Class<? extends Service> getInterface() {
        return RetryQueueService.class;
    }
    
    private final BlockingQueue<RetryWorkItem> retryQueue = new DelayQueue<RetryWorkItem>();
    
    public void addWorkItem(final XCallable<?> callable, int delay) {
        final RetryWorkItem workItem = new RetryWorkItem(callable, delay);
        if (workerExecutor) {
            if (!retryQueue.contains(workItem)) {
                retryQueue.offer(workItem);
                LOG.info(" +++ " + workItem.toString());
            }
        }
    }
    
    public boolean contains(final XCallable<?> callable) {
        final RetryWorkItem workItem = new RetryWorkItem(callable, 0);
        return retryQueue.contains(workItem);
    }
    
    public void process() {
        final Collection<RetryWorkItem> expired = new ArrayList<RetryWorkItem>();
        drainTo2(expired, retryQueue);
        
        if (expired.size() > 0) {
            LOG.info(" rpoll() : queue [" + retryQueue.size() + "], expried[" + expired.size() + "] "
                    + expired.toString());
        }
        for (final RetryWorkItem workItem : expired) {
            XCallable<?> callable = workItem.getElement();
            try {
                callable.call();
            } catch (Exception e) {
                LOG.info("Error , ", e);
            }
        }
    }
    
    private final ReentrantLock lock = new ReentrantLock();
    
    public int drainTo2(Collection<RetryWorkItem> c, BlockingQueue<RetryWorkItem> q) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int n = 0;
            for (RetryWorkItem item : q) {
                if (item.getDelay(TimeUnit.MILLISECONDS) <= 0) {
                    c.add(item);
                    n++;
                }
            }
            if (n > 0)
                q.removeAll(c);
            return n;
        } finally {
            lock.unlock();
        }
    }
    
    public int drainToOrg(Collection<RetryWorkItem> c, BlockingQueue<RetryWorkItem> q) {
        if (c == null)
            throw new NullPointerException();
        if (c == this)
            throw new IllegalArgumentException();
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int n = 0;
            for (;;) {
                RetryWorkItem first = q.peek();
                if (first == null || first.getDelay(TimeUnit.MILLISECONDS) > 0)
                    break;
                c.add(q.poll());
                ++n;
            }
            if (n > 0)
                q.removeAll(c);
            return n;
        } finally {
            lock.unlock();
        }
    }
    
    public class RetryWorkItem implements Delayed {
        private final long origin;
        private final long delay;
        private final XCallable<?> workItem;
        
        public RetryWorkItem(final XCallable<?> workItem, final long delay) {
            this.origin = System.currentTimeMillis();
            this.workItem = workItem;
            this.delay = delay;
        }
        
        public XCallable<?> getElement() {
            return workItem;
        }
        
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(delay - (System.currentTimeMillis() - origin), TimeUnit.MILLISECONDS);
        }
        
        @Override
        public int compareTo(Delayed delayed) {
            if (delayed == this) {
                return 0;
            }
            
            if (delayed instanceof RetryWorkItem) {
                long diff = delay - ((RetryWorkItem) delayed).delay;
                return ((diff == 0) ? 0 : ((diff < 0) ? -1 : 1));
            }
            
            long d = (getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
            return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            
            if (obj == null) {
                return false;
            }
            
            if (!(obj instanceof RetryWorkItem)) {
                return false;
            }
            
            final RetryWorkItem other = (RetryWorkItem) obj;
            if (workItem == null) {
                if (other.workItem != null) {
                    return false;
                }
            } else if (!workItem.getKey().equals(other.workItem.getKey())) {
                return false;
            }
            
            return true;
        }
        
        @Override
        public String toString() {
            return workItem.getKey() + ", delay=" + getDelay(TimeUnit.MILLISECONDS);
        }
    }
    
    private Thread createThread() {
        w = new Worker();
        Thread t = new Thread(w);
        boolean workerStarted = false;
        if (t != null) {
            if (t.isAlive()) // precheck that t is startable
                throw new IllegalThreadStateException();
            w.thread = t;
            try {
                t.start();
                workerStarted = true;
            } finally {
                LOG.debug("thread started");
            }
        }
        return t;
    }
    
    private final class Worker implements Runnable {
        
        Thread thread;
        
        volatile boolean startRun = false;
        
        Worker() {
        }
        
        void interruptNow() {
            if (startRun) {
                thread.interrupt();
            }
            startRun = false;
        }
        
        public void run() {
            try {
                startRun = true;
                LOG.info("worker started");
                while (startRun) {
                    process();
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        LOG.info(" worker error : ", e);
                    }
                }
            } finally {
                LOG.info("worker done");
            }
        }
    }
    
}
