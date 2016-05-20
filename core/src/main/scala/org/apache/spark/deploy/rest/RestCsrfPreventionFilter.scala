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

package org.apache.spark.deploy.rest

import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * This filter provides protection against cross site request forgery (CSRF)
 * attacks for REST APIs. Enabling this filter on an endpoint results in the
 * requirement of all client to send a particular HTTP header (X-XSRF-HEADER)
 * with every request. In the absense of this header the filter will reject the
 * attempt as a bad request.
 */
private[spark] class RestCsrfPreventionFilter extends Filter {

  import RestCsrfPreventionFilter._

  def init(filterConfig: FilterConfig): Unit = {}

  def doFilter(
      servletRequest: ServletRequest,
      servletResponse: ServletResponse,
      filterChain: FilterChain): Unit = {
    val httpReq = servletRequest.asInstanceOf[HttpServletRequest]
    if(ignoreMethods.contains(httpReq.getMethod) || httpReq.getHeader(X_XSRF_HEADER).nonEmpty) {
      filterChain.doFilter(servletRequest, servletResponse)
    } else {
      servletResponse.asInstanceOf[HttpServletResponse].sendError(
        HttpServletResponse.SC_BAD_REQUEST, "Missing Required Header for CSRF protection.")
    }
  }

  def destroy(): Unit = {}
}

private[spark] object RestCsrfPreventionFilter {

  val X_XSRF_HEADER = "X-XSRF-HEADER"
  val ignoreMethods = Array("GET", "OPTIONS", "HEAD", "TRACE")
}
