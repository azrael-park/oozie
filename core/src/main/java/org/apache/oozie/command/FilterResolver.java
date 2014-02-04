package org.apache.oozie.command;

import org.apache.oozie.BaseEngineException;
import org.apache.oozie.BundleEngineException;
import org.apache.oozie.CoordinatorEngineException;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionInfo;
import org.apache.oozie.WorkflowsInfo;
import org.apache.oozie.XException;
import org.apache.oozie.client.BundleJob;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.executor.jpa.WorkflowActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowsJobGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterResolver {

    private static final XLog LOG = XLog.getLog(FilterResolver.class);

    interface FilterSet {
        String field();
        String validate(String value);
    }

    private static enum JOBS_FILTER implements FilterSet {
        user,
        name,
        group,
        status { public String validate(String value) { return WorkflowJob.Status.valueOf(value.toUpperCase()).name(); } },
        id;

        public String field() { return name(); }
        public String validate(String value) { return value; }
    }

    private static enum WF_ACTIONS_FILTER implements FilterSet {
        id,
        wfId,
        name,
        type,
        status { public String validate(String value) { return WorkflowAction.Status.valueOf(value.toUpperCase()).name(); } };

        public String field() { return name(); }
        public String validate(String value) { return value; }
    }

    private static enum COORD_ACTIONS_FILTER implements FilterSet {
        id,
        jobId,
        status { public String validate(String value) { return CoordinatorAction.Status.valueOf(value.toUpperCase()).name(); } },
        externalId;

        public String field() { return name(); }
        public String validate(String value) { return value; }
    }

    private static enum COORD_FILTER implements FilterSet {
        user,
        name { public String field() { return "appName"; } },
        group,
        status { public String validate(String value) { return CoordinatorJob.Status.valueOf(value.toUpperCase()).name(); } },
        id,
        frequency { public String validate(String value) {
            try {
                return String.valueOf((int) Float.parseFloat(value));
            } catch (NumberFormatException e) {
                return null;
            }
        } },
        unit {
            public String field() { return "timeUnitStr"; }
            public String validate(String value) {
            if (!value.equalsIgnoreCase("months") && !value.equalsIgnoreCase("days")
                    && !value.equalsIgnoreCase("hours") && !value.equalsIgnoreCase("minutes")) {
                return null;
            }
            return value.substring(0, value.length() - 1).toUpperCase();
        } };

        public String field() { return name(); }
        public String validate(String value) { return value; }
    }

    private static enum BUNDLE_FILTER implements FilterSet {
        user,
        name { public String field() { return "appName"; } },
        group,
        status { public String validate(String value) { return BundleJob.Status.valueOf(value.toUpperCase()).name(); } },
        id;

        public String field() { return name(); }
        public String validate(String value) { return value; }
    }

    // actionName=hive, startedRecent=1D, status=SUCCESS
    public static boolean actionExists(String conditions, boolean precondition) {
        try {
            Map<String, List<String>> filter = parseForWFAction(conditions);
            if (precondition && !filter.containsKey(OozieClient.FILTER_STATUS)) {
                filter.put(OozieClient.FILTER_STATUS, Arrays.asList("OK"));    // default
            }
            JPAService jpa = Services.get().get(JPAService.class);
            WorkflowActionInfo actions = jpa.execute(new WorkflowActionsGetJPAExecutor(filter, true));
            return actions.getTotal() > 0;
        } catch (XException e) {
            LOG.warn(e);
        }
        return false;
    }

    public static boolean workflowExists(String conditions, boolean precondition) {
        try {
            Map<String, List<String>> filter = parseForJobs(conditions);
            if (precondition && !filter.containsKey(OozieClient.FILTER_STATUS)) {
                filter.put(OozieClient.FILTER_STATUS, Arrays.asList("SUCCEEDED"));    // default
            }
            JPAService jpa = Services.get().get(JPAService.class);
            WorkflowsInfo jobs = jpa.execute(new WorkflowsJobGetJPAExecutor(filter, true));
            return jobs.getTotal() > 0;
        } catch (XException e) {
            LOG.warn(e);
        }
        return false;
    }

    public static Map<String, List<String>> parseForWFAction(String conditions) throws DagEngineException {
        try {
            return parseFilter(WF_ACTIONS_FILTER.class, conditions);
        } catch (BaseEngineException e) {
            throw new DagEngineException(e);
        }
    }

    public static Map<String, List<String>> parseForCoordAction(String conditions) throws CoordinatorEngineException {
        try {
            return parseFilter(COORD_ACTIONS_FILTER.class, conditions);
        } catch (BaseEngineException e) {
            throw new CoordinatorEngineException(e);
        }
    }

    public static Map<String, List<String>> parseForJobs(String conditions) throws DagEngineException {
        try {
            return parseFilter(JOBS_FILTER.class, conditions);
        } catch (BaseEngineException e) {
            throw new DagEngineException(e);
        }
    }

    public static Map<String, List<String>> parseForCoords(String conditions) throws CoordinatorEngineException {
        Map<String, List<String>> result;
        try {
            result = parseFilter(COORD_FILTER.class, conditions);
        } catch (BaseEngineException e) {
            throw new CoordinatorEngineException(e);
        }
        if (!result.containsKey("frequency") && result.containsKey("timeUnitStr")) {
            throw new CoordinatorEngineException(ErrorCode.E0420, conditions, "time unit should be added only when "
                        + "frequency is specified. Either specify frequency also or else remove the time unit");
        }
        if (result.containsKey("frequency") && !result.containsKey("timeUnitStr")) {
            result.put("timeUnitStr", Arrays.asList("MINUTE"));
        }
        return result;
    }

    public static Map<String, List<String>> parseForBundles(String conditions) throws BundleEngineException {
        try {
            return parseFilter(BUNDLE_FILTER.class, conditions);
        } catch (BaseEngineException e) {
            throw new BundleEngineException(e);
        }
    }

    /**
     * Validate a jobs conditions.
     *
     * @param conditions conditions to validate.
     * @return the parsed conditions.
     * @throws org.apache.oozie.DagEngineException thrown if the conditions is invalid.
     */
    private static <T extends Enum<T> & FilterSet> Map<String, List<String>> parseFilter(Class<T> filter, String conditions) throws BaseEngineException {
        Map<String, List<String>> result = new HashMap<String, List<String>>();
        if (conditions == null || conditions.isEmpty()) {
            return result;
        }
        for (String condition : conditions.split(";")) {
            String[] keyValue = condition.split("=");
            if (keyValue.length != 2) {
                throw new BaseEngineException(ErrorCode.E0420, conditions, "elements must be consist of name=value pairs");
            }
            FilterSet key = valueOf(filter, keyValue[0].trim());
            List<String> list = result.get(key.field());
            if (list == null) {
                result.put(key.field(), list = new ArrayList<String>());
            }
            for (String split : keyValue[1].split("\\|")) {
                list.add(validate(key, split.trim()));
            }
        }
        return result;
    }

    private static <T extends Enum<T> & FilterSet> T valueOf(Class<T> filter, String key) throws BaseEngineException {
        try {
            return Enum.valueOf(filter, key);
        } catch (Exception e) {
            throw new BaseEngineException(ErrorCode.E0421, key);
        }
    }

    private static String validate(FilterSet filter, String value) throws BaseEngineException {
        String validate;
        try {
            validate = filter.validate(value);
        } catch (Exception e) {
            throw new BaseEngineException(ErrorCode.E0422, value);
        }
        if (validate == null) {
            throw new BaseEngineException(ErrorCode.E0422, value);
        }
        return validate;
    }
}
