package org.apache.oozie.servlet;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.HiveQueryStatusBean;
import org.apache.oozie.client.rest.JsonBean;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.HiveAccessService;
import org.apache.oozie.service.Services;
import org.json.simple.JSONArray;
import org.json.simple.JSONStreamAware;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HiveServlet extends JsonRestServlet {

    private static final String INSTRUMENTATION_NAME = "hive";

    private static final ResourceInfo[] RESOURCES_INFO = new ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new ResourceInfo("*", Arrays.asList("PUT"), Arrays.asList(
                new ParameterInfo(RestConstants.HIVE_ACTION, String.class, true, Arrays.asList("PUT"))));
    }

    public HiveServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    /**
     * Return information about SLA Events.
     */
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String action = request.getParameter(RestConstants.HIVE_ACTION);
        if (action.equals(RestConstants.HIVE_ACTION_STATUS)) {
            HiveAccessService hive = Services.get().get(HiveAccessService.class);

            String wfID = request.getParameter(RestConstants.HIVE_STATUS_WF_ID);
            String actionID = request.getParameter(RestConstants.HIVE_STATUS_ACTION_ID);
            String queryID = request.getParameter(RestConstants.HIVE_STATUS_QUERY_ID);
            String stageID = request.getParameter(RestConstants.HIVE_STATUS_STAGE_ID);
            String jobID = request.getParameter(RestConstants.HIVE_STATUS_JOB_ID);
            try {
                if (wfID != null) {
                    List<HiveQueryStatusBean> status = hive.getStatusForWorkflow(wfID);
                    sendJsonResponse(response, HttpServletResponse.SC_OK, toJSONArray(status));
                } else if (jobID != null) {
                    HiveQueryStatusBean status = hive.getStatusForJob(jobID);
                    sendJsonResponse(response, HttpServletResponse.SC_OK, (JSONStreamAware) status);
                } else if (stageID != null) {
                    HiveQueryStatusBean status = hive.getStatusForStage(actionID, queryID, jobID);
                    sendJsonResponse(response, HttpServletResponse.SC_OK, (JSONStreamAware) status);
                } else if (queryID != null) {
                    List<HiveQueryStatusBean> status = hive.getStatusForQuery(actionID, queryID);
                    sendJsonResponse(response, HttpServletResponse.SC_OK, toJSONArray(status));
                } else if (actionID != null) {
                    List<HiveQueryStatusBean> status = hive.getStatusForAction(actionID);
                    sendJsonResponse(response, HttpServletResponse.SC_OK, toJSONArray(status));
                } else {
                    throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0302, action);
                }
            } catch (JPAExecutorException e) {
                throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0307, e);
            }
        } else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                    RestConstants.ACTION_PARAM, action);
        }
    }

    @SuppressWarnings("unchecked")
    public static JSONArray toJSONArray(List<? extends JsonBean> workflows) {
        JSONArray array = new JSONArray();
        if (workflows != null) {
            for (JsonBean node : workflows) {
                array.add(node.toJSONObject());
            }
        }
        return array;
    }
}
