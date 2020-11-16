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

package org.apache.spark.ui

import java.util.{Enumeration, Map => JMap}
import javax.servlet._
import javax.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}

import scala.collection.JavaConverters._

import org.apache.commons.text.StringEscapeUtils

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.config.UI._

/**
 * A servlet filter that implements HTTP security features. The following actions are taken
 * for every request:
 *
 * - perform access control of authenticated requests.
 * - check request data for disallowed content (e.g. things that could be used to create XSS
 *   attacks).
 * - set response headers to prevent certain kinds of attacks.
 *
 * Request parameters are sanitized so that HTML content is escaped, and disallowed content is
 * removed.
 */
private class HttpSecurityFilter(
    conf: SparkConf,
    securityMgr: SecurityManager) extends Filter {

  override def destroy(): Unit = { }

  override def init(config: FilterConfig): Unit = { }

  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    val hreq = req.asInstanceOf[HttpServletRequest]
    val hres = res.asInstanceOf[HttpServletResponse]
    hres.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")

    val requestUser = hreq.getRemoteUser()

    // The doAs parameter allows proxy servers (e.g. Knox) to impersonate other users. For
    // that to be allowed, the authenticated user needs to be an admin.
    val effectiveUser = Option(hreq.getParameter("doAs"))
      .map { proxy =>
        if (requestUser != proxy && !securityMgr.checkAdminPermissions(requestUser)) {
          hres.sendError(HttpServletResponse.SC_FORBIDDEN,
            s"User $requestUser is not allowed to impersonate others.")
          return
        }
        proxy
      }
      .getOrElse(requestUser)

    if (!securityMgr.checkUIViewPermissions(effectiveUser)) {
      hres.sendError(HttpServletResponse.SC_FORBIDDEN,
        s"User $effectiveUser is not authorized to access this page.")
      return
    }

    // SPARK-10589 avoid frame-related click-jacking vulnerability, using X-Frame-Options
    // (see http://tools.ietf.org/html/rfc7034). By default allow framing only from the
    // same origin, but allow framing for a specific named URI.
    // Example: spark.ui.allowFramingFrom = https://example.com/
    val xFrameOptionsValue = conf.getOption("spark.ui.allowFramingFrom")
      .map { uri => s"ALLOW-FROM $uri" }
      .getOrElse("SAMEORIGIN")

    hres.setHeader("X-Frame-Options", xFrameOptionsValue)
    hres.setHeader("X-XSS-Protection", conf.get(UI_X_XSS_PROTECTION))
    if (conf.get(UI_X_CONTENT_TYPE_OPTIONS)) {
      hres.setHeader("X-Content-Type-Options", "nosniff")
    }
    if (hreq.getScheme() == "https") {
      conf.get(UI_STRICT_TRANSPORT_SECURITY).foreach(
        hres.setHeader("Strict-Transport-Security", _))
    }

    chain.doFilter(new XssSafeRequest(hreq, effectiveUser), res)
  }

}

private class XssSafeRequest(req: HttpServletRequest, effectiveUser: String)
  extends HttpServletRequestWrapper(req) {

  private val NEWLINE_AND_SINGLE_QUOTE_REGEX = raw"(?i)(\r\n|\n|\r|%0D%0A|%0A|%0D|'|%27)".r

  private val parameterMap: Map[String, Array[String]] = {
    super.getParameterMap().asScala.map { case (name, values) =>
      stripXSS(name) -> values.map(stripXSS)
    }.toMap
  }

  override def getRemoteUser(): String = effectiveUser

  override def getParameterMap(): JMap[String, Array[String]] = parameterMap.asJava

  override def getParameterNames(): Enumeration[String] = {
    parameterMap.keys.iterator.asJavaEnumeration
  }

  override def getParameterValues(name: String): Array[String] = parameterMap.get(name).orNull

  override def getParameter(name: String): String = {
    parameterMap.get(name).flatMap(_.headOption).orNull
  }

  private def stripXSS(str: String): String = {
    if (str != null) {
      // Remove new lines and single quotes, followed by escaping HTML version 4.0
      StringEscapeUtils.escapeHtml4(NEWLINE_AND_SINGLE_QUOTE_REGEX.replaceAllIn(str, ""))
    } else {
      null
    }
  }

}
