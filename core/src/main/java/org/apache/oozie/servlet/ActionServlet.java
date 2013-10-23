package org.apache.oozie.servlet;

import org.apache.oozie.DagEngine;
import org.apache.oozie.DagEngineException;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.rest.RestConstants;
import org.apache.oozie.service.DagEngineService;
import org.apache.oozie.service.Services;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;

public class ActionServlet extends JsonRestServlet {

    private static final String INSTRUMENTATION_NAME = "action";

    private static final ResourceInfo[] RESOURCES_INFO = new ResourceInfo[1];

    static {
        RESOURCES_INFO[0] = new ResourceInfo("*", Arrays.asList("GET"), Arrays.asList(
                new ParameterInfo(RestConstants.ACTION_PARAM, String.class, true, Arrays.asList("GET"))));
    }

    public ActionServlet() {
        super(INSTRUMENTATION_NAME, RESOURCES_INFO);
    }

    @Override
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
                dagEngine.streamLog(actionID, response.getWriter());
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
