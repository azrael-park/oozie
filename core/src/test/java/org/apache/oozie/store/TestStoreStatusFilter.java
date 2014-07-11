package org.apache.oozie.store;

import junit.framework.TestCase;
import org.apache.oozie.BaseEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.executor.jpa.WorkflowsJobGetJPAExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestStoreStatusFilter extends TestCase {

    public void testWFFilter() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        String filterStr = "user=test;group=test-group;name=test-app;status=RUNNING;status=SUSPENDED;";
        Map<String, List<String>> filter = StoreStatusFilter.parseFilter(filterStr, StoreStatusFilter.FILTER.WF);
        printFilter(filter);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.wfSeletStr);

        System.out.println("sb : \n" +sb.toString());
        System.out.println("orArray : " + orArray.toString());
        System.out.println("colArray : " + colArray.toString());
        System.out.println("valArray : " + valArray.toString());

        assertEquals(4, orArray.size());
        assertEquals(true, orArray.contains("statusStr"));
        assertEquals(true, orArray.contains("appName"));
        assertEquals(true, orArray.contains("user"));

        assertEquals("[statusStr, statusStr, appName, user]", orArray.toString());
        assertEquals("[statusStr0, statusStr1, appName2, user3]", colArray.toString());
        assertEquals("[RUNNING, SUSPENDED, test-app, test]", valArray.toString());

        String condition = StoreStatusFilter.wfSeletStr
                + " where w.statusStr IN (:statusStr0, :statusStr1) and w.appName IN (:appName2) and w.user IN (:user3)";

        assertEquals(condition, sb.toString());
    }

    public void testWFFilter2() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        String filterStr = "user=test;name=test-app;id=XXX-W";
        Map<String, List<String>> filter = StoreStatusFilter.parseFilter(filterStr, StoreStatusFilter.FILTER.WF);
        printFilter(filter);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.wfSeletStr);

        System.out.println("sb : \n" +sb.toString());
        System.out.println("orArray : " + orArray.toString());
        System.out.println("colArray : " + colArray.toString());
        System.out.println("valArray : " + valArray.toString());

        assertEquals(3, orArray.size());
        assertEquals(true, orArray.contains("id"));
        assertEquals(true, orArray.contains("appName"));
        assertEquals(true, orArray.contains("user"));

        assertEquals("[id, appName, user]", orArray.toString());
        assertEquals("[id0, appName1, user2]", colArray.toString());
        assertEquals("[XXX-W, test-app, test]", valArray.toString());

        String condition = StoreStatusFilter.wfSeletStr
                + " where w.id IN (:id0) and w.appName IN (:appName1) and w.user IN (:user2)";
        assertEquals(condition, sb.toString());
    }

    public void testCoordFilter1() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        filter.put(OozieClient.FILTER_ID, Arrays.asList("XXX-C"));
        filter.put(OozieClient.FILTER_UNIT, Arrays.asList("MINUTE"));
        filter.put(OozieClient.FILTER_FREQUENCY, Arrays.asList("10"));
        filter.put(OozieClient.FILTER_USER, Arrays.asList("test-user"));
        printFilter(filter);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.coordSeletStr);

        System.out.println("sb : \n" +sb.toString());
        System.out.println("orArray : " + orArray.toString());
        System.out.println("colArray : " + colArray.toString());
        System.out.println("valArray : " + valArray.toString());

        assertEquals(4, orArray.size());
        assertEquals("[id, timeUnitStr, frequency, user]", orArray.toString());
        assertEquals("[id0, timeUnitStr1, frequency2, user3]", colArray.toString());
        assertEquals("[XXX-C, MINUTE, 10, test-user]", valArray.toString());

        String condition = StoreStatusFilter.coordSeletStr
                + " where w.id IN (:id0) and w.timeUnitStr IN (:timeUnitStr1) and w.frequency IN (:frequency2) and w.user IN (:user3)";
        assertEquals(condition, sb.toString());
    }

    public void testCoordFilter2() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        filter.put(OozieClient.FILTER_ID, Arrays.asList("XXX-C"));
        filter.put(OozieClient.FILTER_UNIT, Arrays.asList("MINUTE"));
        filter.put(OozieClient.FILTER_FREQUENCY, Arrays.asList("60"));
        filter.put(OozieClient.FILTER_USER, Arrays.asList("test-user"));

        printFilter(filter);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.coordSeletStr);

        System.out.println("sb : \n" +sb.toString());
        System.out.println("orArray : " + orArray.toString());
        System.out.println("colArray : " + colArray.toString());
        System.out.println("valArray : " + valArray.toString());

        assertEquals(4, orArray.size());
        assertEquals("[id, timeUnitStr, frequency, user]", orArray.toString());
        assertEquals("[id0, timeUnitStr1, frequency2, user3]", colArray.toString());
        assertEquals("[XXX-C, MINUTE, 60, test-user]", valArray.toString());

        String condition = StoreStatusFilter.coordSeletStr
                + " where w.id IN (:id0) and w.timeUnitStr IN (:timeUnitStr1) and w.frequency IN (:frequency2) and w.user IN (:user3)";
        assertEquals(condition, sb.toString());
    }

    public void testBundleFilter() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        String filterStr = "user=test-user;name=test-app;status=RUNNING;status=SUSPENDED;";
        Map<String, List<String>> filter = StoreStatusFilter.parseFilter(filterStr, StoreStatusFilter.FILTER.BUNDLE);
        printFilter(filter);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.bundleSeletStr);

        System.out.println("sb : \n" +sb.toString());
        System.out.println("orArray : " + orArray.toString());
        System.out.println("colArray : " + colArray.toString());
        System.out.println("valArray : " + valArray.toString());

        assertEquals(4, orArray.size());
        assertEquals(true, orArray.contains("statusStr"));
        assertEquals(true, orArray.contains("appName"));
        assertEquals(true, orArray.contains("user"));

        assertEquals("[statusStr, statusStr, appName, user]", orArray.toString());
        assertEquals("[statusStr0, statusStr1, appName2, user3]", colArray.toString());
        assertEquals("[RUNNING, SUSPENDED, test-app, test-user]", valArray.toString());

        String condition = StoreStatusFilter.bundleSeletStr
                + " where w.statusStr IN (:statusStr0, :statusStr1) and w.appName IN (:appName2) and w.user IN (:user3)";

        assertEquals(condition, sb.toString());
    }


    private void printFilter(Map<String,List<String>> filter) {
        System.out.println("----- printFilter start ----");
        for (Map.Entry<String, List<String>> entry: filter.entrySet()) {
            String column = entry.getKey();
            List<String> values =  entry.getValue();
            System.out.println(column + " : " + values.toString());
        }
    }
}
