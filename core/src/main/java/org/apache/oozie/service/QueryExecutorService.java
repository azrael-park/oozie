package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.util.PriorityDelayQueue;
import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class QueryExecutorService implements Service {
    
    public XLog LOG = XLog.getLog(getClass());
    
    private ThreadPoolExecutor executor;

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "QueryExecutorService.";

    public static final String CONF_THREADS = CONF_PREFIX + "threads";

    private final BlockingQueue<Runnable> queryQueue = new ArrayBlockingQueue<Runnable>(100);

    private boolean isDestroying = false;

    @Override
    public void init(Services services) throws ServiceException {
        Configuration conf = services.getConf();

        int threads = conf.getInt(CONF_THREADS, 60);
        executor = new ThreadPoolExecutor(threads, threads, threads, TimeUnit.SECONDS, (BlockingQueue) queryQueue);
        for (int i = 0; i < threads; i++) {
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException ex) {
                        LOG.warn("Could not warm up threadpool {0}", ex.getMessage(), ex);
                    }
                }
            });
        }
    }
    
    @Override
    public void destroy() {

        isDestroying = true;
        try {
            long limit = System.currentTimeMillis() + 30 * 1000;// 30 seconds
            executor.shutdown();
            queryQueue.clear();
            while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                LOG.debug("Waiting for executor to shutdown");
                if (System.currentTimeMillis() > limit) {
                    executor.shutdownNow();
                    LOG.warn("Gave up, continuing without waiting for executor to shutdown");
                    break;
                }
            }
        }
        catch (InterruptedException ex) {
            LOG.warn(ex);
        }
    }
    
    @Override
    public Class<? extends Service> getInterface() {
        return QueryExecutorService.class;
    }

    public synchronized boolean queue(Runnable runnable) {
        if (executor.isShutdown()) {
            LOG.warn("Executor shutting down, ignoring queueing of [{0}]", runnable.toString());
            return false;
        }
        LOG.info(" ++++ " + runnable.toString());
        executor.execute(runnable);
        return true;
    }

    public List<String> getQueueDump() {
        List<String> list = new ArrayList<String>();
        for (Runnable runnable : queryQueue) {
            if (runnable.toString() == null) {
                continue;
            }
            list.add(runnable.toString());
        }
        return list;
    }
}
