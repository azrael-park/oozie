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
package org.apache.oozie.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.oozie.BaseEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.util.XLog;

public class StoreStatusFilter {
    public static final String coordSeletStr = "Select w.id, w.appName, w.statusStr, w.user, w.group, w.startTimestamp, " +
            "w.endTimestamp, w.appPath, w.concurrency, w.frequency, w.lastActionTimestamp, w.nextMaterializedTimestamp, " +
            "w.createdTimestamp, w.timeUnitStr, w.timeZone, w.timeOut from CoordinatorJobBean w";

    public static final String coordCountStr = "Select count(w) from CoordinatorJobBean w";

    public static final String wfSeletStr = "Select w.id, w.appName, w.statusStr, w.run, w.user, w.group, w.createdTimestamp, " +
            "w.startTimestamp, w.lastModifiedTimestamp, w.endTimestamp, w.externalId, w.parentId from WorkflowJobBean w";

    public static final String wfCountStr = "Select count(w) from WorkflowJobBean w";

    public static final String bundleSeletStr = "Select w.id, w.appName, w.appPath, w.conf, w.statusStr, w.kickoffTimestamp, " +
            "w.startTimestamp, w.endTimestamp, w.pauseTimestamp, w.createdTimestamp, w.user, w.group, w.timeUnitStr, " +
            "w.timeOut from BundleJobBean w";

    public static final String bundleCountStr = "Select count(w) from BundleJobBean w";

    public static void filter(Map<String, List<String>> filter, List<String> orArray, List<String> colArray,
                              List<String> valArray, StringBuilder sb, String seletStr) {
        int index = 0;
        AtomicBoolean started = new AtomicBoolean(false);

        sb.append(seletStr);
        for (Map.Entry<String, List<String>> entry : filter.entrySet()) {
            if (entry.getKey().equals(OozieClient.FILTER_GROUP)) {
                XLog.getLog(StoreStatusFilter.class).warn("Filter by 'group' is not supported anymore");
                continue;
            }
            String colName = entry.getKey();
            if (colName.equals(OozieClient.FILTER_NAME)) {
                colName = "appName";
            } else if (colName.equals(OozieClient.FILTER_STATUS)) {
                colName = "statusStr";
            } else if (colName.equals(OozieClient.FILTER_UNIT)) {
                colName = "timeUnitStr";
            }
            index = generateQueryCondition(index, started, colName, entry.getValue(), orArray, colArray,
                    valArray, sb);
        }
    }

    private static int generateQueryCondition(int index, AtomicBoolean started, String column, List<String> colValues,
                                         List<String> orArray, List<String> colArray, List<String> valArray, StringBuilder sb) {
        List<String> values = colValues;
        String colName = column;
        boolean isEnabled = started.get();

        for (int i = 0; i < values.size(); i++) {
            if (i == 0) {
                if (!isEnabled) {
                    sb.append(" where");
                    isEnabled = true;
                } else {
                    sb.append(" and");
                }
                sb.append(" w." + colName + " IN (");
            }
            String colVar = colName + index;
            sb.append(":" + colVar + ", ");
            if (i == values.size() -1) {
                sb.replace(sb.length() - 2, sb.length(), ")");
            }

            index++;
            valArray.add(values.get(i));
            orArray.add(colName);
            colArray.add(colVar);
        }
        started.set(isEnabled);
        return index;
    }

    public static enum FILTER {
        WF{
            public void checkStatus(String status) {
                WorkflowJob.Status.valueOf(status);
            }
        },
        COORD{
            public Set<String> getFilterNames() {
                Set<String> filterNames = new HashSet<String>();
                filterNames.add(OozieClient.FILTER_USER);
                filterNames.add(OozieClient.FILTER_NAME);
                filterNames.add(OozieClient.FILTER_GROUP);
                filterNames.add(OozieClient.FILTER_STATUS);
                filterNames.add(OozieClient.FILTER_ID);
                filterNames.add(OozieClient.FILTER_FREQUENCY);
                filterNames.add(OozieClient.FILTER_UNIT);
                return filterNames;
            }
            public void checkStatus(String status) {
                CoordinatorJob.Status.valueOf(status);
            }
        },
        BUNDLE{
            public void checkStatus(String status) {
                BundleJob.Status.valueOf(status);
            }
        }
        ;
        public String field() {
            return name();
        }

        public String validate(String value) {
            return value;
        }

        public Set<String> getFilterNames() {
            Set<String> filterNames = new HashSet<String>();
            filterNames.add(OozieClient.FILTER_USER);
            filterNames.add(OozieClient.FILTER_NAME);
            filterNames.add(OozieClient.FILTER_GROUP);
            filterNames.add(OozieClient.FILTER_STATUS);
            filterNames.add(OozieClient.FILTER_ID);
            return filterNames;
        }

        public boolean isValidFilterName(String filterName) {
            return getFilterNames().contains(filterName);
        }

        public abstract void checkStatus(String status);

    }

    public static Map<String, List<String>> parseFilter(String filter, FILTER jobFilter) throws BaseEngineException{
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        if (filter == null || filter.isEmpty()) {
            return map;
        }

        for (String condition: filter.split(";")) {
            String[] pair = condition.split("=");
            if (pair.length != 2) {
                throw new BaseEngineException(ErrorCode.E0420, filter, "elements must be name=value pairs");
            }
            if (!jobFilter.isValidFilterName(pair[0].toLowerCase())) {
                throw new BaseEngineException(ErrorCode.E0420, filter, XLog.format("invalid name [{0}]",
                        pair[0]));
            }
            if (pair[0].equals("status")) {
                try {
                    jobFilter.checkStatus(pair[1]);
                }
                catch (IllegalArgumentException ex) {
                    throw new BaseEngineException(ErrorCode.E0420, filter, XLog.format(
                            "invalid status [{0}]", pair[1]));
                }
            }
            List<String> list = map.get(pair[0]);
            if (list == null) {
                list = new ArrayList<String>();
                map.put(pair[0], list);
            }
            list.add(pair[1]);
        }

        return map;
    }
}
