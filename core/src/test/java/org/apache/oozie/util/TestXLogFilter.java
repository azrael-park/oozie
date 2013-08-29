/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.oozie.util;

import org.apache.oozie.service.ServiceException;
import org.apache.oozie.service.Services;

import java.util.ArrayList;

import org.apache.oozie.test.XTestCase;

public class TestXLogFilter extends XTestCase {
    public void testXLogFileter() throws ServiceException {
        Services services = new Services();
        services.init();
        try {
            XLogStreamer.Filter xf2 = new XLogStreamer.Filter();
            xf2.constructPattern();
            ArrayList<String> a = new ArrayList<String>();
            a.add("2009-06-24 02:43:13,958");
            a.add(" DEBUG");
            a.add(" WorkflowRunnerCallable:323 - " + XLog.Info.get().createPrefix() + " test log");
            assertEquals(true, xf2.matches(a));
        }
        finally {
            services.destroy();
        }

        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter("USER");
        XLogStreamer.Filter.defineParameter("GROUP");
        XLogStreamer.Filter.defineParameter("TOKEN");
        XLogStreamer.Filter.defineParameter("APP");
        XLogStreamer.Filter.defineParameter("JOB");
        XLogStreamer.Filter.defineParameter("ACTION");
        XLogStreamer.Filter xf = new XLogStreamer.Filter();

        assertEquals(7, matches(xf));
        xf.setLogLevel(XLog.Level.WARN.toString());
        assertEquals(2, matches(xf));

        xf.setLogLevel(XLog.Level.WARN.toString());
        xf.setParameter("APP", "example-forkjoinwf");
        assertEquals(0, matches(xf));

        xf.setLogLevel(XLog.Level.DEBUG.toString() + "|" + XLog.Level.INFO.toString());
        xf.setParameter("JOB", "14-200904160239--example-forkjoinwf");
        assertEquals(2, matches(xf));

        XLogStreamer.Filter xf1 = new XLogStreamer.Filter();
        xf1.setParameter("USER", "oozie");
        assertEquals(3, matches(xf1));

        xf1.setParameter("GROUP", "oozie");
        assertEquals(2, matches(xf1));

        xf1.setParameter("TOKEN", "MYtoken");
        assertEquals(1, matches(xf1));
    }

    private int matches(XLogStreamer.Filter xf) {
        xf.constructPattern();
        ArrayList<String> a = new ArrayList<String>();
        a.add("2009-06-24 02:43:13,958 DEBUG WorkflowRunnerCallable:323 - USER[oozie] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] End workflow state change");
        a.add("2009-06-24 02:43:13,961  INFO WorkflowRunnerCallable:317 - USER[-] GROUP[-] TOKEN[-] APP[example-forkjoinwf] JOB[14-200904160239--example-forkjoinwf] ACTION[-] [org.apache.oozie.core.command.WorkflowRunnerCallable] released lock");
        a.add("2009-06-24 02:43:13,986  WARN JobClient:539 - Use GenericOptionsParser for parsing the arguments. Applications should implement Tool for the same.");
        a.add("2009-06-24 02:43:14,431  WARN JobClient:661 - No job jar file set.  User classes may not be found. See JobConf(Class) or JobConf#setJar(String).");
        a.add("2009-06-24 02:43:14,505  INFO ActionExecutorCallable:317 - USER[oozie] GROUP[oozie] TOKEN[-] APP[-] JOB[-] ACTION[-] Released Lock");
        a.add("2009-06-24 02:43:19,344 DEBUG PendingSignalsCallable:323 - USER[oozie] GROUP[oozie] TOKEN[MYtoken] APP[-] JOB[-] ACTION[-] Number of pending signals to check [0]");
        a.add("2009-06-24 02:43:29,151 DEBUG PendingActionsCallable:323 - USER[-] GROUP[-] TOKEN[-] APP[-] JOB[-] ACTION[-] Number of pending actions [0] ");
        int matchCnt = 0;
        for (int i = 0; i < a.size(); i++) {
            if (xf.matches(xf.splitLogMessage(a.get(i)))) {
                matchCnt++;
            }
        }
        return matchCnt;
    }

    public void testXLogFileterTrimLog() throws ServiceException {
        Services services = new Services();
        services.init();
        try {
            XLogStreamer.Filter xf2 = new XLogStreamer.Filter();
            xf2.constructPattern();
            ArrayList<String> a = new ArrayList<String>();
            a.add("2009-06-24 02:43:13,958 DEBUG");
            a.add(" WorkflowRunnerCallable:323 - " + XLog.Info.get().createPrefix() + " test log");
            assertEquals(true, xf2.matches(a));
        }
        finally {
            services.destroy();
        }

        XLogStreamer.Filter.reset();
        XLogStreamer.Filter.defineParameter("USER");
        XLogStreamer.Filter.defineParameter("GROUP");
        XLogStreamer.Filter.defineParameter("TOKEN");
        XLogStreamer.Filter.defineParameter("APP");
        XLogStreamer.Filter.defineParameter("JOB");
        XLogStreamer.Filter.defineParameter("ACTION");

        ArrayList<String> nonTrimedLog =  createTestLog();
        XLogStreamer.Filter xf = new XLogStreamer.Filter();
        assertEquals(6, matchesWithLog(xf, nonTrimedLog));

        xf.setLogLevel(XLog.Level.DEBUG.toString() + "|" + XLog.Level.INFO.toString());
        xf.setParameter("JOB", "0000000-140122104650403-oozie-seoe-W");
        assertEquals(2, matchesWithLog(xf, nonTrimedLog));

        XLogStreamer.Filter xf1 = new XLogStreamer.Filter();
        xf1.setLogLevel(XLog.Level.DEBUG.toString() + "|" + XLog.Level.INFO.toString());
        xf1.setParameter("JOB", "0000002-140122104650403-oozie-seoe-W");
        assertEquals(3, matchesWithLog(xf1, nonTrimedLog));
        xf1.setParameter("ACTION", "start");
        assertEquals(2, matchesWithLog(xf1, nonTrimedLog));


        ArrayList<String> trimedLog =  createTestLogTrim();
        xf = new XLogStreamer.Filter();
        assertEquals(6, matchesWithLog(xf, trimedLog));

        xf.setLogLevel(XLog.Level.DEBUG.toString() + "|" + XLog.Level.INFO.toString());
        xf.setParameter("JOB", "0000000-140122104650403-oozie-seoe-W");
        assertEquals(2, matchesWithLog(xf, trimedLog));

        xf1 = new XLogStreamer.Filter();
        xf1.setLogLevel(XLog.Level.DEBUG.toString() + "|" + XLog.Level.INFO.toString());
        xf1.setParameter("JOB", "0000002-140122104650403-oozie-seoe-W");
        assertEquals(3, matchesWithLog(xf1, trimedLog));
        xf1.setParameter("ACTION", "start");
        assertEquals(2, matchesWithLog(xf1, trimedLog));

    }

    private int matchesWithLog(XLogStreamer.Filter xf, ArrayList<String> logs) {
        xf.constructPattern();
        ArrayList<String> a = logs;
        int matchCnt = 0;
        for (int i = 0; i < a.size(); i++) {
            if (xf.matches(xf.splitLogMessage(a.get(i)))) {
                matchCnt++;
            }
        }
        return matchCnt;
    }

    private int matchesWithTrimLog(XLogStreamer.Filter xf) {
        xf.constructPattern();
        ArrayList<String> a = createTestLogTrim();
        int matchCnt = 0;
        for (int i = 0; i < a.size(); i++) {
            if (xf.matches(xf.splitLogMessage(a.get(i)))) {
                matchCnt++;
            }
        }
        return matchCnt;
    }

    private int matchesWithNonTrimLog(XLogStreamer.Filter xf) {
        xf.constructPattern();
        ArrayList<String> a = createTestLog();
        int matchCnt = 0;
        for (int i = 0; i < a.size(); i++) {
            if (xf.matches(xf.splitLogMessage(a.get(i)))) {
                matchCnt++;
            }
        }
        return matchCnt;
    }

    private ArrayList<String> createTestLog(){
        ArrayList<String> a = new ArrayList<String>();
        a.add("2014-01-22 10:47:06,603 DEBUG ActionStartXCommand:568 - USER[seoeun] GROUP[user] TOKEN[] APP[test] JOB[0000000-140122104650403-oozie-seoe-W] ACTION[start] Execute command [action.start]");
        a.add("2014-01-22 10:47:06,603  INFO ActionStartXCommand:562 - USER[seoeun] GROUP[user] TOKEN[] APP[test] JOB[0000000-140122104650403-oozie-seoe-W] ACTION[start] STARTED ActionStartXCommand");
        a.add("2014-01-22 10:47:06,612 DEBUG ActionStartXCommand:568 - USER[seoeun] GROUP[user] TOKEN[] APP[test] JOB[0000001-140122104650403-oozie-seoe-W] ACTION[shell] Start, name");
        a.add("2014-01-22 10:47:06,603 DEBUG ActionStartXCommand:568 - USER[ndap] GROUP[user] TOKEN[] APP[test] JOB[0000002-140122104650403-oozie-seoe-W] ACTION[start] Execute command [action.start]");
        a.add("2014-01-22 10:47:06,603  INFO ActionStartXCommand:562 - USER[ndap] GROUP[user] TOKEN[] APP[test] JOB[0000002-140122104650403-oozie-seoe-W] ACTION[start] STARTED ActionStartXCommand");
        a.add("2014-01-22 10:47:06,612 DEBUG ActionStartXCommand:568 - USER[seoeun] GROUP[user] TOKEN[] APP[test] JOB[0000002-140122104650403-oozie-seoe-W] ACTION[hive] Start, name");
        return a;
    }

    private ArrayList<String> createTestLogTrim(){
        ArrayList<String> a = new ArrayList<String>();
        a.add("2014-01-22 11:09:43,680  INFO ActionEndXCommand:562 - USER[seoeun] GROUP[user] APP[test] JOB[0000000-140122104650403-oozie-seoe-W] ACTION[start]  STARTED ActionEndXCommand : status[DONE]");
        a.add("2014-01-22 11:09:43,680  INFO ActionEndXCommand:562 - USER[seoeun] GROUP[user] APP[test] JOB[0000000-140122104650403-oozie-seoe-W] ACTION[start]  STARTED ActionEndXCommand : status[DONE]");
        a.add("2014-01-22 11:09:43,680  INFO ActionEndXCommand:562 - USER[seoeun] GROUP[user] APP[test] JOB[0000001-140122104650403-oozie-seoe-W] ACTION[shell]  STARTED ActionEndXCommand : status[DONE]");
        a.add("2014-01-22 11:09:43,680  INFO ActionEndXCommand:562 - USER[seoeun] GROUP[user] APP[test] JOB[0000002-140122104650403-oozie-seoe-W] ACTION[start]  STARTED ActionEndXCommand : status[DONE]");
        a.add("2014-01-22 11:09:43,680  INFO ActionEndXCommand:562 - USER[seoeun] GROUP[user] APP[test] JOB[0000002-140122104650403-oozie-seoe-W] ACTION[start]  STARTED ActionEndXCommand : status[DONE]");
        a.add("2014-01-22 11:09:43,680  INFO ActionEndXCommand:562 - USER[seoeun] GROUP[user] APP[test] JOB[0000002-140122104650403-oozie-seoe-W] ACTION[hive]  STARTED ActionEndXCommand : status[DONE]");
        return a;
    }

}
