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

import org.apache.hadoop.security.UserGroupInformation;
import javax.servlet.http.HttpServletRequest;

public class ProxyFileForward extends ProxyForwardServlet {
  /** For java.io.Serializable */
  private static final long serialVersionUID = 1L;

  /** {@inheritDoc} */
  @Override
  protected String buildForwardPath(HttpServletRequest request, String pathInfo) {
    String path = "/streamFile";
    path += request.getPathInfo();
    String userID = (String) request.
        getAttribute("org.apache.hadoop.hdfsproxy.authorized.userID");
    UserGroupInformation ugi = ProxyUtil.getProxyUGIFor(userID);
    if (ugi != null) {
      path += "?ugi=" + ugi.getShortUserName();
    }
    return path;
  }
  
}
