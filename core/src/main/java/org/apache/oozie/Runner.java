package org.apache.oozie;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Enumeration;

import static org.apache.oozie.service.XLogService.DEFAULT_LOG4J_PROPERTIES;
import static org.apache.oozie.service.XLogService.LOG4J_FILE;

public class Runner {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        System.out.println(Arrays.toString(((URLClassLoader) conf.getClassLoader()).getURLs()));
        System.out.println("core-default.xml = " + conf.getClassLoader().getResource("core-default.xml"));
        System.out.println("core-site.xml    = " + conf.getClassLoader().getResource("core-site.xml"));

        String log4j = System.getProperty(LOG4J_FILE, DEFAULT_LOG4J_PROPERTIES);

        ClassLoader loader = Runner.class.getClassLoader();
        Enumeration<URL> enums = loader.getResources(log4j);
        while (enums.hasMoreElements()) {
            System.out.println("log4j = " + enums.nextElement());
        }

        String path = args.length > 0 ? args[0] : "/home/navis/apache/oss-oozie";
        if (new File(path).exists()) {
            System.out.println("oozie.home = " + path);
            System.setProperty(Services.OOZIE_HOME_DIR, path);
            System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, path + "/conf");
            System.setProperty(ConfigurationService.OOZIE_DATA_DIR, path + "/data");
        }

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
