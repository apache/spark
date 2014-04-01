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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AuthorizationFilter implements Filter {
  public static final Log LOG = LogFactory.getLog(AuthorizationFilter.class);

  private static final Pattern HDFS_PATH_PATTERN = Pattern
      .compile("(^hdfs://([\\w\\-]+(\\.)?)+:\\d+|^hdfs://([\\w\\-]+(\\.)?)+)");

  /** Pattern for a filter to find out if a request is HFTP/HSFTP request */
  protected static final Pattern HFTP_PATTERN = Pattern
      .compile("^(/listPaths|/data|/streamFile|/file)$");

  protected String contextPath;

  protected String namenode;

  /** {@inheritDoc} **/
  public void init(FilterConfig filterConfig) throws ServletException {
    contextPath = filterConfig.getServletContext().getContextPath();
    Configuration conf = new Configuration(false);
    conf.addResource("hdfsproxy-default.xml");
    conf.addResource("hdfsproxy-site.xml");
    namenode = conf.get("fs.default.name");
  }

  /** {@inheritDoc} **/
  @SuppressWarnings("unchecked")
  public void doFilter(ServletRequest request,
                       ServletResponse response,
                       FilterChain chain)
      throws IOException, ServletException {

    HttpServletResponse rsp = (HttpServletResponse) response;
    HttpServletRequest rqst = (HttpServletRequest) request;

    String userId = getUserId(request);
    String groups = getGroups(request);
    List<Path> allowedPaths = getAllowedPaths(request);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser(userId);

    String filePath = getPathFromRequest(rqst);

    if (filePath == null || !checkHdfsPath(filePath, allowedPaths)) {
      String msg = "User " + userId + " (" + groups
          + ") is not authorized to access path " + filePath;
      LOG.warn(msg);
      rsp.sendError(HttpServletResponse.SC_FORBIDDEN, msg);
      return;
    }
    request.setAttribute("authorized.ugi", ugi);

    chain.doFilter(request, response);
  }

  protected String getUserId(ServletRequest request) {
     return (String)request.
         getAttribute("org.apache.hadoop.hdfsproxy.authorized.userID");
  }

   protected String getGroups(ServletRequest request) {
     UserGroupInformation ugi = UserGroupInformation.
         createRemoteUser(getUserId(request));
     return Arrays.toString(ugi.getGroupNames());
   }

  protected List<Path> getAllowedPaths(ServletRequest request) {
     return (List<Path>)request.
         getAttribute("org.apache.hadoop.hdfsproxy.authorized.paths");
  }
  
   private String getPathFromRequest(HttpServletRequest rqst) {
    String filePath = null;
    // check request path
    String servletPath = rqst.getServletPath();
    if (HFTP_PATTERN.matcher(servletPath).matches()) {
        // file path as part of the URL
        filePath = rqst.getPathInfo() != null ? rqst.getPathInfo() : "/";
    }
    return filePath;
  }

  /** check that the requested path is listed in the ldap entry
   * @param pathInfo - Path to check access
   * @param ldapPaths - List of paths allowed access
   * @return true if access allowed, false otherwise */
  public boolean checkHdfsPath(String pathInfo,
                               List<Path> ldapPaths) {
    if (pathInfo == null || pathInfo.length() == 0) {
      LOG.info("Can't get file path from the request");
      return false;
    }
    for (Path ldapPathVar : ldapPaths) {
      String ldapPath = ldapPathVar.toString();
      if (isPathQualified(ldapPath) &&
          isPathAuthroized(ldapPath)) {
        String allowedPath = extractPath(ldapPath);
        if (pathInfo.startsWith(allowedPath))
          return true;
      } else {
        if (pathInfo.startsWith(ldapPath))
          return true;
      }
    }
    return false;
  }

  private String extractPath(String ldapPath) {
    return HDFS_PATH_PATTERN.split(ldapPath)[1];
  }

  private boolean isPathAuthroized(String pathStr) {
    Matcher namenodeMatcher = HDFS_PATH_PATTERN.matcher(pathStr);
    return namenodeMatcher.find() && namenodeMatcher.group().contains(namenode);
  }

  private boolean isPathQualified(String pathStr) {
    if (pathStr == null || pathStr.trim().isEmpty()) {
      return false;
    } else {
      return HDFS_PATH_PATTERN.matcher(pathStr).find();
    }
  }

  /** {@inheritDoc} **/
  public void destroy() {
  }
}
