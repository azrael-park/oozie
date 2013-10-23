package org.apache.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.XLogService;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;

public class Runner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        System.out.println(Arrays.toString(((URLClassLoader) conf.getClassLoader()).getURLs()));
        System.out.println(conf.getClassLoader().getResource("core-default.xml"));
        System.out.println(conf.getClassLoader().getResource("core-site.xml"));
        System.out.println(conf.getClassLoader().getResource("org/codehaus/jackson/map/JsonMappingException.class"));

        System.setProperty(XLogService.LOG4J_FILE, "oozie-log4j.properties");
        ClassLoader loader = Runner.class.getClassLoader();

        Enumeration<URL> enums = loader.getResources("log4j.properties");
        while (enums.hasMoreElements()) {
            System.out.println("-- [Runner/main] " + enums.nextElement());
        }

        System.setProperty(Services.OOZIE_HOME_DIR, "/home/navis/oozie");
        System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, "/home/navis/oozie/conf");
        System.setProperty(ConfigurationService.OOZIE_DATA_DIR, "/home/navis/oozie/data");

//        LocalOozie.start();
//        SimpleClient client = new SimpleClient(LocalOozie.getClient());
        SimpleServer.start();
        SimpleClient client = new SimpleClient(SimpleServer.getClient());
        try {
            client.execute(args.length > 0 ? args[0] : null);
        } finally {
//            LocalOozie.stop();
            SimpleServer.stop();
        }
    }
}
