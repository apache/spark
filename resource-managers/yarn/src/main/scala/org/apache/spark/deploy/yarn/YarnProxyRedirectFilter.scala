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

package org.apache.spark.deploy.yarn

import jakarta.servlet._
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.apache.spark.internal.Logging

/**
 * A filter to be used in the Spark History Server for redirecting YARN proxy requests to the
 * main SHS address. This is useful for applications that are using the history server as the
 * tracking URL, since the SHS-generated pages cannot be rendered in that case without extra
 * configuration to set up a proxy base URI (meaning the SHS cannot be ever used directly).
 */
class YarnProxyRedirectFilter extends Filter with Logging {

  import YarnProxyRedirectFilter._

  override def destroy(): Unit = { }

  override def init(config: FilterConfig): Unit = { }

  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    val hreq = req.asInstanceOf[HttpServletRequest]

    // The YARN proxy will send a request with the "proxy-user" cookie set to the YARN's client
    // user name. We don't expect any other clients to set this cookie, since the SHS does not
    // use cookies for anything.
    Option(hreq.getCookies()).flatMap(_.find(_.getName() == COOKIE_NAME)) match {
      case Some(_) =>
        doRedirect(hreq, res.asInstanceOf[HttpServletResponse])

      case _ =>
        chain.doFilter(req, res)
    }
  }

  private def doRedirect(req: HttpServletRequest, res: HttpServletResponse): Unit = {
    val redirect = req.getRequestURL().toString()

    // Need a client-side redirect instead of an HTTP one, otherwise the YARN proxy itself
    // will handle the redirect and get into an infinite loop.
    val content = s"""
      |<html xmlns="http://www.w3.org/1999/xhtml">
      |<head>
      |  <title>Spark History Server Redirect</title>
      |  <meta http-equiv="refresh" content="0;URL='$redirect'" />
      |</head>
      |<body>
      |  <p>The requested page can be found at: <a href="$redirect">$redirect</a>.</p>
      |</body>
      |</html>
      """.stripMargin

    logDebug(s"Redirecting YARN proxy request to $redirect.")
    res.setStatus(HttpServletResponse.SC_OK)
    res.setContentType("text/html")
    res.getWriter().write(content)
  }

}

private[spark] object YarnProxyRedirectFilter {
  val COOKIE_NAME = "proxy-user"
}
