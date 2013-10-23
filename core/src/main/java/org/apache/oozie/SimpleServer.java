package org.apache.oozie;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.service.CallbackService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.webapp.WebAppContext;

public class SimpleServer {

    private static Server server;
    private static boolean localOozieActive;

    private static String address;

    public synchronized static void start() throws Exception {
        if (localOozieActive) {
            throw new IllegalStateException("LocalOozie is already initialized");
        }

        String log4jFile = System.getProperty(XLogService.LOG4J_FILE, null);

        String war = System.getProperty("oozie.web", "webapp/src/main/webapp");
        String oozieLocalLog = System.getProperty("oozielocal.log", null);
        if (log4jFile == null) {
            System.setProperty(XLogService.LOG4J_FILE, "localoozie-log4j.properties");
        }
        if (oozieLocalLog == null) {
            System.setProperty("oozielocal.log", "./oozielocal.log");
        }

        localOozieActive = true;
        new Services().init();

        if (log4jFile != null) {
            System.setProperty(XLogService.LOG4J_FILE, log4jFile);
        } else {
            System.getProperties().remove(XLogService.LOG4J_FILE);
        }
        if (oozieLocalLog != null) {
            System.setProperty("oozielocal.log", oozieLocalLog);
        } else {
            System.getProperties().remove("oozielocal.log");
        }

        Server server = new Server();

        SocketConnector connector = new SocketConnector();
        connector.setPort(11000);

        server.addConnector(connector);
        WebAppContext root = new WebAppContext();

        root.setWar(war);
        root.setContextPath("/oozie");

        server.addHandler(root);
        server.start();

        SimpleServer.server = server;
        SimpleServer.address = "http://" + server.getConnectors()[0].getName() + root.getContextPath();

        String callback = address + "/callback";
        Services.get().getConf().set(CallbackService.CONF_BASE_URL, callback);
        XLog.getLog(SimpleServer.class).info("SimpleServer started callback set to [{0}]", callback);
    }

    public synchronized static void stop() {
        RuntimeException thrown = null;
        try {
            if (server != null) {
                server.stop();
            }
        } catch (Exception ex) {
            thrown = new RuntimeException(ex);
        }
        server = null;
        XLog.getLog(SimpleServer.class).info("LocalOozie stopped");
        try {
            Services.get().destroy();
        } catch (RuntimeException ex) {
            if (thrown != null) {
                thrown = ex;
            }
        }
        localOozieActive = false;
        if (thrown != null) {
            throw thrown;
        }
    }

    public static OozieClient getClient() {
        return getClient(System.getProperty("user.name"));
    }

    public static OozieClient getClient(String user) {
        if (!localOozieActive) {
            throw new IllegalStateException("LocalOozie is not initialized");
        }
        ParamChecker.notEmpty(user, "user");
        return new OozieClient(address);
    }
}
