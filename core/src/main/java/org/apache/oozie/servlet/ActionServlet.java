package org.apache.oozie.servlet;

import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XConfiguration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class ActionServlet extends JsonRestServlet {

    private static final String INSTRUMENTATION_NAME = "action";

    private static final ResourceInfo[] RESOURCES_INFO = new ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new ResourceInfo("*", Arrays.asList("PUT", "GET"), Arrays.asList(new ParameterInfo(
                RestConstants.ACTION_PARAM, String.class, true, Arrays.asList("PUT", "GET"))));
    }

    public ActionServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String actionID = getResourceName(request);
        String action = request.getParameter(RestConstants.ACTION_PARAM);

        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

        if (action.equals(RestConstants.ACTION_SUSPEND)) {
            try {
                dagEngine.suspend(actionID);
            } catch (DagEngineException e) {
                throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e);
            }
        } else if (action.equals(RestConstants.ACTION_RESUME)) {
            try {
                dagEngine.resume(actionID);
            } catch (DagEngineException e) {
                throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e);
            }
        } else if (action.equals(RestConstants.ACTION_SHOW_LOG)) {
            response.setContentType(TEXT_UTF8);
            try {
                dagEngine.streamLog(actionID, response.getWriter(), request.getParameterMap());
            } catch (DagEngineException e) {
                throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e);
            }
        } else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                    RestConstants.ACTION_PARAM, action);
        }
        response.setStatus(HttpServletResponse.SC_OK);
    }

    /**
     * Return information about SLA Events.
     */
    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String actionID = getResourceName(request);
        String action = request.getParameter(RestConstants.ACTION_PARAM);

        validateContentType(request, RestConstants.XML_CONTENT_TYPE);
        XConfiguration conf = new XConfiguration(request.getInputStream());
        DagEngine dagEngine = Services.get().get(DagEngineService.class).getDagEngine(getUser(request));

        if (action.equals(RestConstants.ACTION_UPDATE)) {
            Map<String, String> params = new HashMap<String, String>();
            for (Map.Entry<String, String> entry : conf) {
                params.put(entry.getKey(), entry.getValue());
            }
            try {
                dagEngine.update(actionID, params);
            } catch (DagEngineException e) {
                throw new XServletException(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, e);
            }
        } else {
            throw new XServletException(HttpServletResponse.SC_BAD_REQUEST, ErrorCode.E0303,
                    RestConstants.ACTION_PARAM, action);
        }
        response.setStatus(HttpServletResponse.SC_OK);
    }

}
