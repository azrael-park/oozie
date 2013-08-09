package org.apache.oozie.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.Utils;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.action.hive.HiveSession;
import org.apache.oozie.action.hive.HiveStatus;
import org.apache.oozie.action.hive.HiveTClient;
import org.apache.oozie.action.hive.HiveTClientV1;
import org.apache.oozie.action.hive.HiveTClientV2;
import org.apache.oozie.executor.jpa.HiveStatusGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.util.XLog;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.oozie.action.ActionExecutorException.ErrorType;

public class HiveAccessService implements Service {

    final XLog LOG = XLog.getLog(HiveAccessService.class);

    // wfID : action --> HiveStatus
    final Map<String, Map<String, HiveStatus>> hiveStatus = new HashMap<String, Map<String, HiveStatus>>();
    final Map<String, AdminConnection> adminConnections = new HashMap<String, AdminConnection>();

    UUIDService uuid;

    public void init(Services services) throws ServiceException {
        uuid = Services.get().get(UUIDService.class);
    }

    public synchronized void destroy() {
        for (Map<String, HiveStatus> value : new ArrayList<Map<String, HiveStatus>>(hiveStatus.values())) {
            for (HiveStatus status : value.values()) {
                try {
                    status.shutdown(false);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        hiveStatus.clear();

        for (AdminConnection connection : adminConnections.values()) {
            try {
                connection.client.destroy();
            } catch (Exception e) {
                // ignore
            }
        }
        adminConnections.clear();
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

    public synchronized HiveStatus unregister(String actionID) {
        String wfID = uuid.getId(actionID);
        Map<String, HiveStatus> map = hiveStatus.get(wfID);
        if (map != null) {
            return map.remove(uuid.getChildName(actionID));
        }
        return null;
    }

    public synchronized Map<String, HiveStatus> unregisterWF(String wfID) {
        return hiveStatus.remove(wfID);
    }

    public boolean actionFinished(String actionID) {
        HiveStatus status = unregister(actionID);
        if (status != null) {
            return status.shutdown(false);
        }
        return false;
    }

    public void jobFinished(String wfID) {
        Map<String, HiveStatus> finished = unregisterWF(wfID);
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

    public HiveTClient clientFor(String address) throws ActionExecutorException {
        return getHiveTClient(Utils.parseURI(URI.create(address)));
    }

    private HiveTClient getHiveTClient(Utils.JdbcConnectionParams connParams) throws ActionExecutorException {
        if (connParams.getScheme() != null && !connParams.getScheme().isEmpty() && !connParams.getScheme().equals("hive")) {
            Configuration conf = Services.get().get(ConfigurationService.class).getConf();
            if(conf.getBoolean(HiveSession.PING_ENABLED, false)){
                checkSocketForV2(connParams);
            }
            return createClientForV2(connParams);
        }
        return createClientForV1(connParams);
    }

    private HiveTClient createClientForV1(Utils.JdbcConnectionParams params) throws ActionExecutorException {
        try {
            TSocket protocol = new TSocket(params.getHost(), params.getPort());
            protocol.open();
            return new HiveTClientV1(protocol, new ThriftHive.Client(new TBinaryProtocol(protocol)), params);
        } catch (Throwable e) {
            throw new ActionExecutorException(ErrorType.TRANSIENT, "HIVE-002", "failed to connect hive server {0}", toAddress(params), e);
        }
    }

    public HiveTClient createClientForV2(Utils.JdbcConnectionParams params) throws ActionExecutorException {
        HiveConnection connection = new HiveConnection();
        try {
            connection.initialize(params, new Properties());
            return new HiveTClientV2(connection, params);
        } catch (Throwable e) {
            throw new ActionExecutorException(ErrorType.TRANSIENT, "HIVE-002", "failed to connect hive server {0}", toAddress(params), e);
        }
    }

    private void checkSocketForV2(Utils.JdbcConnectionParams params) throws ActionExecutorException {
        HiveConnection connection = null;
        Utils.JdbcConnectionParams pingParams = new Utils.JdbcConnectionParams();
        pingParams.setHost(params.getHost());
        pingParams.setPort(params.getPort());
        pingParams.setScheme(params.getScheme());
        Map<String, String> sesseionVars = new HashMap<String, String>();
        sesseionVars.put("socketTimeout", String.valueOf(HiveSession.PING_TIMEOUT));
        pingParams.setSessionVars(sesseionVars);
        try {
            connection = new HiveConnection();
            connection.initialize(pingParams, new Properties());
        } catch (Exception e){
            LOG.warn("hive server does not respond {0}", toAddress(pingParams));
            throw new ActionExecutorException(ErrorType.TRANSIENT, "HIVE-002", "failed to connect hive server {0}", toAddress(params), e);
        } finally {
            if(connection != null){
                try{
                    connection.close();
                } catch (Exception e){
                    //ignore
                }
            }

        }
    }

    public boolean ping(Utils.JdbcConnectionParams params, int timeout) throws Exception {
        final String key = toStringKey(params);
        final AdminConnection connection = getAdminConnection(key);
        if (!connection.lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
            return false;
        }
        try {
            return pingInLock(params, timeout, connection);
        } finally {
            connection.lock.unlock();
        }
    }

    private boolean pingInLock(Utils.JdbcConnectionParams params, int timeout, AdminConnection connection)
            throws Exception {
        if (connection.lastSuccess + timeout > System.currentTimeMillis()) {
            return true;
        }
        if (connection.client == null) {
            connection.client = getHiveTClient(params);
        }
        try {
            if (connection.client.ping(timeout)) {
                connection.lastSuccess = System.currentTimeMillis();
                return true;
            }
            return false;
        } catch (Exception ex) {
            connection.client.destroy();
            connection.client = null;
            throw ex;
        }
    }

    private synchronized AdminConnection getAdminConnection(String key) {
        AdminConnection connection = adminConnections.get(key);
        if (connection == null) {
            adminConnections.put(key, connection = new AdminConnection());
        }
        return connection;
    }

    private static class AdminConnection {
        final ReentrantLock lock = new ReentrantLock();
        HiveTClient client;
        long lastSuccess;
    }

    private static final java.lang.String HIVE_AUTH_TYPE = "auth";
    private static final java.lang.String HIVE_AUTH_USER = "user";
    private static final java.lang.String HIVE_AUTH_PRINCIPAL = "principal";

    private String toAddress(Utils.JdbcConnectionParams params) {
        StringBuilder builder = new StringBuilder();
        builder.append(params.getHost()).append(':');
        builder.append(params.getPort());
        return builder.toString();
    }

    private String toStringKey(Utils.JdbcConnectionParams params) {
        Map<String, String> sessionVars = params.getSessionVars();
        StringBuilder builder = new StringBuilder();
        builder.append(params.getHost()).append(':');
        builder.append(params.getPort()).append(':');
        builder.append(sessionVars.get(HIVE_AUTH_TYPE)).append(':');
        builder.append(sessionVars.get(HIVE_AUTH_USER)).append(':');
        builder.append(sessionVars.get(HIVE_AUTH_PRINCIPAL));
        return builder.toString();
    }
}
