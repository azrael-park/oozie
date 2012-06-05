package org.apache.oozie.service;

import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hive.HiveSession;
import org.apache.oozie.executor.jpa.HiveStatusGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.oozie.action.ActionExecutorException.ErrorType;

public class HiveAccessService implements Service {

    private static final int DEFAULT_PORT = 10000;

    Map<String, Map<String, HiveSession>> hiveStatus;   // wfID : action --> HiveSession
    ThriftHive.Client.Factory syncFactory;
    UUIDService uuid;

    @Override
    public void init(Services services) throws ServiceException {
        syncFactory = new ThriftHive.Client.Factory();
        hiveStatus = new HashMap<String, Map<String, HiveSession>>();
        uuid = Services.get().get(UUIDService.class);
    }

    @Override
    public synchronized void destroy() {
        if (hiveStatus != null) {
            for (Map<String, HiveSession> value : hiveStatus.values()) {
                for (HiveSession session : value.values()) {
                    session.shutdown();
                }
            }
        }
    }

    @Override
    public Class<? extends Service> getInterface() {
        return HiveAccessService.class;
    }

    public synchronized void register(String actionID, HiveSession session) throws ActionExecutorException {
        String wfID = uuid.getId(actionID);
        String action = uuid.getChildName(actionID);
        Map<String, HiveSession> map = hiveStatus.get(wfID);
        if (map == null) {
            hiveStatus.put(wfID, map = new LinkedHashMap<String, HiveSession>());
        }
        map.put(action, session);
    }

    public synchronized void unregister(String wfID) {
        hiveStatus.remove(wfID);
    }

    public HiveSession getRunningSession(String actionID) throws ActionExecutorException {
        HiveSession session = peekRunningStatus(actionID);
        if (session == null) {
            throw new ActionExecutorException(ErrorType.ERROR, "HIVE-003", "hive status is not registered for {0}", actionID);
        }
        return session;
    }

    public HiveSession peekRunningStatus(String actionID) {
        String wfID = uuid.getId(actionID);
        Map<String, HiveSession> map = hiveStatus.get(wfID);
        if (map != null) {
            String action = uuid.getChildName(actionID);
            return map.get(action);
        }
        return null;
    }

    public List<HiveQueryStatusBean> getStatusForWorkflow(String wfID) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        return jpaService.execute(new HiveStatusGetJPAExecutor(wfID));
    }

    public List<HiveQueryStatusBean> getStatusForAction(String actionID) throws JPAExecutorException {
        HiveSession session = peekRunningStatus(actionID);
        if (session != null) {
            return session.getStatus();
        }
        String wfID = uuid.getId(actionID);
        String action = uuid.getChildName(actionID);

        JPAService jpaService = Services.get().get(JPAService.class);
        return jpaService.execute(new HiveStatusGetJPAExecutor(wfID, action));
    }

    public List<HiveQueryStatusBean> getStatusForQuery(String actionID, String queryID) throws JPAExecutorException {
        HiveSession session = peekRunningStatus(actionID);
        if (session != null) {
            return session.getStatus(queryID);
        }
        String wfID = uuid.getId(actionID);
        String action = uuid.getChildName(actionID);

        JPAService jpaService = Services.get().get(JPAService.class);
        return jpaService.execute(new HiveStatusGetJPAExecutor(wfID, action, queryID));
    }

    public HiveQueryStatusBean getStatusForStage(String actionID, String queryID, String stageID) throws JPAExecutorException {
        HiveSession session = peekRunningStatus(actionID);
        if (session != null) {
            return session.getStatus(queryID, stageID);
        }
        String wfID = uuid.getId(actionID);
        String action = uuid.getChildName(actionID);

        JPAService jpaService = Services.get().get(JPAService.class);
        List<HiveQueryStatusBean> result = jpaService.execute(new HiveStatusGetJPAExecutor(wfID, action, queryID, stageID));
        return result != null && !result.isEmpty() ? result.get(0) : null;
    }

    public HiveQueryStatusBean getStatusForJob(String jobID) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        List<HiveQueryStatusBean> result = jpaService.execute(new HiveStatusGetJPAExecutor(jobID, false));
        return result != null && !result.isEmpty() ? result.get(0) : null;
    }

    public ThriftHive.Client clientFor(String address) throws ActionExecutorException {
        //FIXME http://localhost:10000/default
        if(address.startsWith("http://")){
            address = address.substring(7);
        }
        if(address.endsWith("default")){
            address = address.substring(0, address.indexOf("default")-1);
        }
        int index = address.indexOf(":");
        String host = index < 0 ? address : address.substring(0, index);
        int port = index < 0 ? DEFAULT_PORT : Integer.valueOf(address.substring(index + 1));
        try {
            TSocket protocol = new TSocket(host, port);
            protocol.open();
            return syncFactory.getClient(new TBinaryProtocol(protocol));
        } catch (Throwable e) {
            throw new ActionExecutorException(ErrorType.TRANSIENT, "HIVE-002", "failed to connect hive server {0}", address, e);
        }
    }
}
