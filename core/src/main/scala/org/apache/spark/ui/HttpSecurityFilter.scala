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

import scala.jdk.CollectionConverters._

import jakarta.servlet._
import jakarta.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}
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

  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    val hreq = req.asInstanceOf[HttpServletRequest]
    val hres = res.asInstanceOf[HttpServletResponse]
    hres.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")

    val cspNonce = CspNonce.generate()
    try {
      // SPARK-10589 avoid frame-related click-jacking vulnerability.
      // Use CSP frame-ancestors as the primary mechanism (supported by all modern browsers),
      // with X-Frame-Options: SAMEORIGIN as a fallback for legacy clients.
      // Note: X-Frame-Options ALLOW-FROM is deprecated and ignored by modern browsers
      // (Chrome, Firefox, Edge, Safari), so frame-ancestors is used instead.
      val frameAncestors = conf.get(UI_ALLOW_FRAMING_FROM)
        .filterNot(v => v.equalsIgnoreCase("SAMEORIGIN") || v.equalsIgnoreCase("DENY"))
        .map { uri =>
          // Sanitize the URI: truncate at semicolons to prevent CSP directive injection,
          // and strip newlines to prevent header injection.
          val sanitized = uri.replaceAll("[\\r\\n]+", "").split(";", 2)(0).trim
          s"frame-ancestors 'self' $sanitized"
        }
        .getOrElse("frame-ancestors 'self'")

      if (conf.get(UI_CONTENT_SECURITY_POLICY_ENABLED)) {
        hres.setHeader("Content-Security-Policy",
          s"default-src 'self'; script-src 'self' 'nonce-$cspNonce'; " +
          s"style-src 'self' 'unsafe-inline'; img-src 'self' data:; " +
          s"object-src 'none'; base-uri 'self'; $frameAncestors;")
      } else {
        // Even when the full CSP is disabled, set frame-ancestors to enforce
        // the allowFramingFrom setting in browsers that support CSP.
        hres.setHeader("Content-Security-Policy", s"$frameAncestors;")
      }

      // X-Frame-Options is set before access control so that even error responses
      // include clickjacking protection.
      hres.setHeader("X-Frame-Options", "SAMEORIGIN")

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

      hres.setHeader("X-XSS-Protection", conf.get(UI_X_XSS_PROTECTION))
      if (conf.get(UI_X_CONTENT_TYPE_OPTIONS)) {
        hres.setHeader("X-Content-Type-Options", "nosniff")
      }
      if (hreq.getScheme() == "https") {
        conf.get(UI_STRICT_TRANSPORT_SECURITY).foreach(
          hres.setHeader("Strict-Transport-Security", _))
      }

      chain.doFilter(new XssSafeRequest(hreq, effectiveUser), res)
    } finally {
      CspNonce.clear()
    }
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
