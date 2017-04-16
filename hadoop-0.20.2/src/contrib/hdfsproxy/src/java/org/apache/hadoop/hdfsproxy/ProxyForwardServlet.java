/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfsproxy;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 *
 */
public class ProxyForwardServlet extends HttpServlet {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  private static Configuration configuration = null;
  public static final Log LOG = LogFactory.getLog(ProxyForwardServlet.class);

  /** {@inheritDoc} */
  @Override
  public void init() throws ServletException {
    ServletContext context = getServletContext();
    configuration = (Configuration) context
        .getAttribute("org.apache.hadoop.hdfsproxy.conf");
  }

  /** {@inheritDoc} */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    String hostname = request.getServerName();

    String version = configuration.get(hostname);
    if (version == null) {
      // extract from hostname directly
      String[] strs = hostname.split("[-\\.]");
      version = "/" + strs[0];
    }

    ServletContext curContext = getServletContext();
    ServletContext dstContext = curContext.getContext(version);

    // avoid infinite forwarding.
    if (dstContext == null
        || "HDFS Proxy Forward".equals(dstContext.getServletContextName())) {
      LOG.error("Context (" + version
          + ".war) non-exist or restricted from access");
      response.sendError(HttpServletResponse.SC_NOT_FOUND);
      return;
    }
    LOG.debug("Request to " + hostname + " is forwarded to version " + version);
    forwardRequest(request, response, dstContext, request.getServletPath());

  }

  /** {@inheritDoc} */
  public void forwardRequest(HttpServletRequest request,
      HttpServletResponse response, ServletContext context, String pathInfo)
      throws IOException, ServletException {
    String path = buildForwardPath(request, pathInfo);
    RequestDispatcher dispatcher = context.getRequestDispatcher(path);
    if (dispatcher == null) {
      LOG.info("There was no such dispatcher: " + path);
      response.sendError(HttpServletResponse.SC_NO_CONTENT);
      return;
    }
    dispatcher.forward(request, response);
  }

  /** {@inheritDoc} */
  protected String buildForwardPath(HttpServletRequest request, String pathInfo) {
    String path = pathInfo;
    if (request.getPathInfo() != null) {
      path += request.getPathInfo();
    }
    if (request.getQueryString() != null) {
      path += "?" + request.getQueryString();
    }
    return path;
  }
}
