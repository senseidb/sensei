package com.senseidb.servlet;

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.configuration.DataConfiguration;
import org.apache.commons.configuration.web.ServletRequestConfiguration;

import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;

public abstract class AbstractSenseiRestServlet extends AbstractSenseiClientServlet {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  abstract protected SenseiRequest buildSenseiRequest(DataConfiguration params) throws Exception;

  @Override
  protected SenseiRequest buildSenseiRequest(HttpServletRequest req) throws Exception {
    DataConfiguration params = new DataConfiguration(new ServletRequestConfiguration(req));
    return buildSenseiRequest(params);
  }

  abstract protected void writeResult(HttpServletRequest httpReq, OutputStream ostream,
      SenseiRequest req, SenseiResult res) throws IOException;

  abstract protected void buildResultString(HttpServletRequest httpReq, OutputStream ostream,
      SenseiSystemInfo info) throws IOException;

  @Override
  protected void writeResult(HttpServletRequest httpReq, SenseiSystemInfo info,
      OutputStream ostream) throws IOException {
    buildResultString(httpReq, ostream, info);
  }

  @Override
  protected void writeResult(HttpServletRequest httpReq, SenseiRequest req, SenseiResult res,
      OutputStream ostream) throws IOException {
    writeResult(httpReq, ostream, req, res);
  }
}
