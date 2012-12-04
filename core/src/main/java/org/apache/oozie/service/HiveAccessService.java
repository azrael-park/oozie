package org.apache.oozie.service;

import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hive.HiveSession;
import org.apache.oozie.action.hive.HiveStatus;
import org.apache.oozie.executor.jpa.HiveStatusGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.XLog;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.oozie.action.ActionExecutorException.ErrorType;

public class HiveAccessService implements Service {

    final XLog LOG = XLog.getLog(HiveAccessService.class);

    private static final int DEFAULT_PORT = 10000;

    Map<String, Map<String, HiveStatus>> hiveStatus;   // wfID : action --> HiveStatus
    ThriftHive.Client.Factory syncFactory;
    UUIDService uuid;

    public void init(Services services) throws ServiceException {
        syncFactory = new ThriftHive.Client.Factory();
        hiveStatus = new HashMap<String, Map<String, HiveStatus>>();
        uuid = Services.get().get(UUIDService.class);
    }

    public synchronized void destroy() {
        if (hiveStatus != null) {
            for (Map<String, HiveStatus> value : hiveStatus.values()) {
                for (HiveStatus status : value.values()) {
                    status.shutdown(false);
                }
            }
            hiveStatus.clear();
        }
        hiveStatus = null;
    }

    public Class<? extends Service> getInterface() {
        return HiveAccessService.class;
    }

    public synchronized void register(String actionID, HiveStatus session) {
        String wfID = uuid.getId(actionID);
        String actionName = uuid.getChildName(actionID);
        Map<String, HiveStatus> map = hiveStatus.get(wfID);
        if (map == null) {
            hiveStatus.put(wfID, map = new LinkedHashMap<String, HiveStatus>());
        }
        map.put(actionName, session);
    }

    public synchronized void unregister(String actionID) {
        String wfID = uuid.getId(actionID);
        String actionName = uuid.getChildName(actionID);
        Map<String, HiveStatus> map = hiveStatus.get(wfID);
        if (map != null) {
            map.remove(actionName);
        }
    }

    public synchronized boolean actionFinished(String actionID) {
        boolean finished = true;
        String wfID = uuid.getId(actionID);
        Map<String, HiveStatus> map = hiveStatus.get(wfID);
        if (map != null) {
            HiveStatus status = map.remove(uuid.getChildName(actionID));
            if (status != null) {
                finished = status.shutdown(false);
            }
        }
        return finished;
    }

    public synchronized void jobFinished(String wfID) {
        Map<String, HiveStatus> finished = hiveStatus.remove(wfID);
        if (finished != null) {
            for (HiveStatus status : finished.values()) {
                status.shutdown(false);
            }
            finished.clear();
        }
    }

    public synchronized HiveStatus peekRunningStatus(String actionID) {
        String wfID = uuid.getId(actionID);
        Map<String, HiveStatus> map = hiveStatus.get(wfID);
        if (map != null) {
            String actionName = uuid.getChildName(actionID);
            return map.get(actionName);
        }
        return null;
    }

    public synchronized HiveStatus accessRunningStatus(String actionID, boolean monitoring, boolean temporal) {
        HiveStatus session = peekRunningStatus(actionID);
        if (session == null) {
            session = temporal ? temporalSession(actionID) : newSession(actionID, monitoring);
            register(actionID, session);
        }
        return session;
    }

    private HiveStatus temporalSession(String actionID) {
        return new HiveStatus(uuid.getId(actionID), uuid.getChildName(actionID));
    }

    private HiveStatus newSession(String actionID, boolean monitoring) {
        return new HiveStatus(uuid.getId(actionID), uuid.getChildName(actionID), monitoring);
    }

    public List<HiveQueryStatusBean> getStatusForWorkflow(String wfID) throws JPAExecutorException {
        JPAService jpaService = Services.get().get(JPAService.class);
        return jpaService.execute(new HiveStatusGetJPAExecutor(wfID));
    }

    public List<HiveQueryStatusBean> getStatusForAction(String actionID) throws JPAExecutorException {
        HiveStatus session = peekRunningStatus(actionID);
        if (session != null) {
            return session.getStatus();
        }
        String wfID = uuid.getId(actionID);
        String actionName = uuid.getChildName(actionID);

        HiveStatusGetJPAExecutor executor = new HiveStatusGetJPAExecutor(wfID, actionName);

        JPAService jpaService = Services.get().get(JPAService.class);
        return jpaService.execute(executor);
    }

    public Map<String, List<String>> getFailedTaskURLs(String id) throws JPAExecutorException {
        String actionName = id.contains("@") ? uuid.getChildName(id) : null;
        List<HiveQueryStatusBean> list = actionName != null ? getStatusForAction(id) : getStatusForWorkflow(id);
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> result = new LinkedHashMap<String, List<String>>();
        for (HiveQueryStatusBean bean : list) {
            if (bean.getFailedTasks() == null || bean.getFailedTasks().isEmpty()) {
                continue;
            }
            StringBuilder builder = new StringBuilder();
            if (actionName == null) {
                builder.append(bean.getActionName()).append(':');
            }
            builder.append(bean.getQueryId()).append(':').append(bean.getStageId());
            List<String> values = new ArrayList<String>();
            for (String entry : bean.getFailedTasks().split(";")) {
                String[] pair = entry.split("=");
                values.add(HiveSession.getTaskLogURL(pair[0], pair[1]));
            }
            result.put(builder.toString(), values);
        }
        return result;
    }

    public List<HiveQueryStatusBean> getStatusForQuery(String actionID, String queryID) throws JPAExecutorException {
        HiveStatus session = peekRunningStatus(actionID);
        if (session != null) {
            return session.getStatus(queryID);
        }
        String wfID = uuid.getId(actionID);
        String actionName = uuid.getChildName(actionID);

        JPAService jpaService = Services.get().get(JPAService.class);
        return jpaService.execute(new HiveStatusGetJPAExecutor(wfID, actionName, queryID));
    }

    public HiveQueryStatusBean getStatusForStage(String actionID, String queryID, String stageID) throws JPAExecutorException {
        HiveStatus session = peekRunningStatus(actionID);
        if (session != null) {
            return session.getStatus(queryID, stageID);
        }
        String wfID = uuid.getId(actionID);
        String actionName = uuid.getChildName(actionID);

        JPAService jpaService = Services.get().get(JPAService.class);
        List<HiveQueryStatusBean> result = jpaService.execute(new HiveStatusGetJPAExecutor(wfID, actionName, queryID, stageID));
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
