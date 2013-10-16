package org.apache.oozie.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JavaMainTest {
    public static final String CONF_OOZIE_JAVA_COMMAND = "command";

    protected void run(String[] args) throws Exception {

        Configuration actionConf = loadActionConf();

        int exitCode = execute(actionConf);
        if (exitCode != 0) {
            // Shell command failed. therefore make the action failed
            throw new LauncherMainException(1);
        }

    }

    /**
     * Execute the shell command
     *
     * @param actionConf
     * @return command exit value
     * @throws java.io.IOException
     */
    private int execute(Configuration actionConf) throws Exception {
        List<String> exec = getCommand(actionConf);
        List<String> args = new ArrayList<String>();
        try {
            return execute(actionConf, getCmdList(exec, args), false, true, true);
        } finally {

        }
    }

    int execute(Configuration actionConf, List<String> cmdArray, boolean quiet, boolean captureOutput, boolean captureError) throws Exception {

        // Getting the Current working dir and setting it to process builder
        File currDir = new File("dummy").getAbsoluteFile().getParentFile();
        if (!quiet) {
            System.out.println("Current working dir " + currDir);
        }

        if (!quiet) {
            printCommand(cmdArray, new HashMap<String, String>()); // For debugging purpose

            System.out.println("=================================================================");
            System.out.println();
            System.out.println(">>> Invoking JavaMainTest command line now >>");
            System.out.println("cmd : "+cmdArray.get(0));
            System.out.println();
            System.out.flush();
        }

        // Execute the Command
        String command = cmdArray.get(0);
        int exitValue = 0;
        if(command.equals("echo")){
            // OK test
            String outData = "javamain-hello-azrael";
            String outFile = System.getProperty("oozie.action.output.dump");
            if (outFile != null) {
                OutputStream os = new FileOutputStream(new File(outFile), true);
                os.write(outData.getBytes());
                os.close();
            }
        } else {
            // ERROR test
            String outData = "javamain-hello-error-azrael";
            String outFile = System.getProperty("oozie.action.error.dump");
            if (outFile != null) {
                OutputStream os = new FileOutputStream(new File(outFile), true);
                os.write(outData.getBytes());
                os.close();
            }
            exitValue = 1;

        }

        if (!quiet) {
            System.out.println("Exit code of the JavaMainTest command " + exitValue);
            System.out.println("<<< Invocation of JavaMainTest command completed <<<");
            System.out.println();
        }
        return exitValue;
    }



    /**
     * Get the shell commands with the arguments
     *
     * @param exec
     * @param args
     * @return command and list of args
     */
    private List<String> getCmdList(List<String> exec, List<String> args) {
        args.addAll(0, exec);
        return args;
    }



    /**
     * Print the command including the arguments as well as the environment
     * setup
     *
     * @param cmdArray :Command Array
     * @param envp :Environment array
     */
    protected void printCommand(List<String> cmdArray, Map<String, String> envp) {
        int i = 0;
        System.out.println("Full Command .. ");
        System.out.println("-------------------------");
        for (String arg : cmdArray) {
            System.out.println(i++ + ":" + arg + ":");
        }

        if (envp != null) {
            System.out.println("List of passing environment");
            System.out.println("-------------------------");
            for (Map.Entry<String, String> entry : envp.entrySet()) {
                System.out.println(entry.getKey() + "=" + entry.getValue() + ":");
            }
        }

    }



    /**
     * Retrieve the executable name that was originally specified to
     * Workflow.xml.
     *
     * @param actionConf
     * @return executable
     */
    protected List<String> getCommand(Configuration actionConf) {
        System.out.println("-----");
        Iterator it = actionConf.iterator();
        while(it.hasNext()){
            Object key = it.next();
            System.out.println(key.toString());
            System.out.println(actionConf.get(key.toString()));
        }
        System.out.println("-----");

        String command = actionConf.get(CONF_OOZIE_JAVA_COMMAND);
        if (command == null) {
            throw new RuntimeException("Action Configuration does not have " + CONF_OOZIE_JAVA_COMMAND + " property");
        }
        List<String> args = new ArrayList<String>();
        args.add(command);
        return args;
    }

    // de-quote
    // should support nested quotation?
    private List<String> parseCommand(String exec) {
        char quote = 0;
        StringBuilder builder = new StringBuilder();
        List<String> command = new ArrayList<String>();
        for (int i = 0; i < exec.length(); i++) {
            char achar = exec.charAt(i);
            if (quote == 0 && achar == '\'' || achar == '"') {
                quote = achar;
                continue;
            } else if (quote != 0 && achar == quote) {
                quote = 0;
                command.add(builder.toString());
                builder.setLength(0);
                continue;
            } else if (quote == 0 && builder.length() > 0 && achar == ' ') {
                command.add(builder.toString());
                builder.setLength(0);
                continue;
            }
            if (builder.length() > 0 || achar != ' ') {
                builder.append(achar);
            }
        }
        if (builder.length() > 0) {
            command.add(builder.toString());
        }
        return command;
    }

    /**
     * Read action configuration passes through action xml file.
     *
     * @return action  Configuration
     * @throws IOException
     */
    protected Configuration loadActionConf() throws IOException {
        System.out.println();
        System.out.println("Oozie JavaMainTest action configuration");
        System.out.println("=================================================================");

        // loading action conf prepared by Oozie
        Configuration actionConf = new Configuration(false);

        String actionXml = System.getProperty("oozie.action.conf.xml");

        if (actionXml == null) {
            throw new RuntimeException("Missing Java System Property [oozie.action.conf.xml]");
        }
        if (!new File(actionXml).exists()) {
            throw new RuntimeException("Action Configuration XML file [" + actionXml + "] does not exist");
        }

        actionConf.addResource(new Path("file:///", actionXml));
        //logMasking("Shell configuration:", new HashSet<String>(), actionConf);
        System.out.println(actionConf.getValByRegex(".*"));
        return actionConf;
    }

    public static void main(String...args) throws Exception {
        JavaMainTest mainTest = new JavaMainTest();
        mainTest.run(args);
    }
}

class LauncherMainException extends Exception {
    private int errorCode;

    public LauncherMainException(int code) {
        errorCode = code;
    }

    int getErrorCode() {
        return errorCode;
    }
}

