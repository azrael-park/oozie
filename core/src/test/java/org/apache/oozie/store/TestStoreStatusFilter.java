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

    /**
     * Test the WF filter that consist of user, group, name, status.
     * @throws Exception
     */
    public void testWFFilter1() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        String filterStr = "user=test;group=test-group;name=test-app;status=RUNNING;status=SUSPENDED;";
        Map<String, List<String>> filter = StoreStatusFilter.parseFilter(filterStr, StoreStatusFilter.FILTER.WF);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.wfSeletStr,
                StoreStatusFilter.FILTER.WF);

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

    /**
     * Test the WF filter that consist of user, name, id, parentid.
     * @throws Exception
     */
    public void testWFFilter2() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        String filterStr = "user=test;name=test-app;id=XXX-W;parentid=XXX-C";
        Map<String, List<String>> filter = StoreStatusFilter.parseFilter(filterStr, StoreStatusFilter.FILTER.WF);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.wfSeletStr,
                StoreStatusFilter.FILTER.WF);

        assertEquals(4, orArray.size());
        assertEquals(true, orArray.contains("id"));
        assertEquals(true, orArray.contains("appName"));
        assertEquals(true, orArray.contains("user"));
        assertEquals(true, orArray.contains("parentId"));

        assertEquals("[id, appName, parentId, user]", orArray.toString());
        assertEquals("[id0, appName1, parentId2, user3]", colArray.toString());
        assertEquals("[XXX-W, test-app, XXX-C, test]", valArray.toString());

        String condition = StoreStatusFilter.wfSeletStr
                + " where w.id IN (:id0) and w.appName IN (:appName1) and w.parentId IN (:parentId2) and w.user IN (:user3)";
        assertEquals(condition, sb.toString());
    }

    /**
     * Test the Coord filter that consist of id, unit, frequency, user.
     * @throws Exception
     */
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

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.coordSeletStr,
                StoreStatusFilter.FILTER.COORD);

        assertEquals(4, orArray.size());
        assertEquals("[id, timeUnitStr, frequency, user]", orArray.toString());
        assertEquals("[id0, timeUnitStr1, frequency2, user3]", colArray.toString());
        assertEquals("[XXX-C, MINUTE, 10, test-user]", valArray.toString());

        String condition = StoreStatusFilter.coordSeletStr
                + " where w.id IN (:id0) and w.timeUnitStr IN (:timeUnitStr1) and w.frequency IN (:frequency2) and w.user IN (:user3)";
        assertEquals(condition, sb.toString());
    }


    /**
     * Test the Coord filter that consist of id, unit, frequency, parentid.
     * @throws Exception
     */
    public void testCoordFilter2() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        Map<String, List<String>> filter = new HashMap<String, List<String>>();
        filter.put(OozieClient.FILTER_ID, Arrays.asList("XXX-C"));
        filter.put(OozieClient.FILTER_UNIT, Arrays.asList("MINUTE"));
        filter.put(OozieClient.FILTER_FREQUENCY, Arrays.asList("60"));
        filter.put(OozieClient.FILTER_PARENTID, Arrays.asList("XXX-B"));

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.coordSeletStr,
                StoreStatusFilter.FILTER.COORD);

        assertEquals(4, orArray.size());
        assertEquals("[id, timeUnitStr, bundleId, frequency]", orArray.toString());
        assertEquals("[id0, timeUnitStr1, bundleId2, frequency3]", colArray.toString());
        assertEquals("[XXX-C, MINUTE, XXX-B, 60]", valArray.toString());

        String condition = StoreStatusFilter.coordSeletStr
                + " where w.id IN (:id0) and w.timeUnitStr IN (:timeUnitStr1) and w.bundleId IN (:bundleId2) and w.frequency IN " +
                "(:frequency3)";
        assertEquals(condition, sb.toString());
    }

    /**
     * Test the Bundle filter that consist of user, name, status.
     * @throws Exception
     */
    public void testBundleFilter1() throws Exception {
        List<String> orArray = new ArrayList<String>();
        List<String> colArray = new ArrayList<String>();
        List<String> valArray = new ArrayList<String>();
        StringBuilder sb = new StringBuilder("");

        String filterStr = "user=test-user;name=test-app;status=RUNNING;status=SUSPENDED;";
        Map<String, List<String>> filter = StoreStatusFilter.parseFilter(filterStr, StoreStatusFilter.FILTER.BUNDLE);

        StoreStatusFilter.filter(filter, orArray, colArray, valArray, sb, StoreStatusFilter.bundleSeletStr,
                StoreStatusFilter.FILTER.BUNDLE);

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


}
