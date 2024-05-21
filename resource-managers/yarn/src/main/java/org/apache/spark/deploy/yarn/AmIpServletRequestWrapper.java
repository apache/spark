/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import java.security.Principal;

// This class is copied from Hadoop 3.4.0
// org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpServletRequestWrapper
//
// Modification:
// Migrate from javax.servlet to jakarta.servlet
public class AmIpServletRequestWrapper extends HttpServletRequestWrapper {
  private final AmIpPrincipal principal;

  public AmIpServletRequestWrapper(HttpServletRequest request,
      AmIpPrincipal principal) {
    super(request);
    this.principal = principal;
  }

  @Override
  public Principal getUserPrincipal() {
    return principal;
  }

  @Override
  public String getRemoteUser() {
    return principal.getName();
  }

  @Override
  public boolean isUserInRole(String role) {
    // No role info so far
    return false;
  }

}
