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

import java.io.IOException
import java.net.{HttpURLConnection, InetAddress, MalformedURLException, UnknownHostException, URL}
import java.security.Principal
import java.util.concurrent.TimeUnit

import jakarta.servlet.{Filter, FilterChain, FilterConfig, ServletException, ServletRequest, ServletResponse}
import jakarta.servlet.http.{HttpServletRequest, HttpServletRequestWrapper, HttpServletResponse}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.internal.Logging

// A functional equivalent rewrite of `org.apache.hadoop.yarn.server.webproxy.amfilter.AmIpFilter`,
// refer to Hadoop 3.4.0
class YarnAMIpFilter extends Filter with Logging {

  import YarnAMIpFilter._

  private[spark] var proxyHosts: Array[String] = _
  private[spark] var proxyAddresses: Set[String] = _
  private[spark] var lastUpdate: Long = 0L
  private[spark] var proxyUriBases: Map[String, String] = _
  private[spark] var rmUrls: Array[String] = _

  @throws[ServletException]
  override def init(conf: FilterConfig): Unit = {
    val legacyProxyHostOpt = Option(conf.getInitParameter(PROXY_HOST))
    val legacyProxyUriBaseOpt = Option(conf.getInitParameter(PROXY_URI_BASE))
    (legacyProxyHostOpt, legacyProxyUriBaseOpt) match {
      // YARN-1811: Maintain for backwards compatibility
      case (Some(legacyProxyHost), Some(legacyProxyUriBase)) =>
        proxyHosts = Array(legacyProxyHost)
        proxyUriBases = Map("dummy" -> legacyProxyUriBase)
      case _ =>
        proxyHosts = conf.getInitParameter(PROXY_HOSTS).split(PROXY_HOSTS_DELIMITER)
        proxyUriBases = conf.getInitParameter(PROXY_URI_BASES).split(PROXY_URI_BASES_DELIMITER)
          .flatMap { proxyUriBase =>
            try {
              val url = new URL(proxyUriBase)
              Some(url.getHost + ":" + url.getPort, proxyUriBase)
            } catch {
              case e: MalformedURLException =>
                logWarning(s"$proxyUriBase does not appear to be a valid URL", e)
                None
            }
          }.toMap
    }

    Option(conf.getInitParameter(RM_HA_URLS)).foreach { rmHaUrls =>
      rmUrls = rmHaUrls.split(RM_HA_URLS_DELIMITER)
    }
  }

  @throws[ServletException]
  protected[spark] def getProxyAddresses: Set[String] = {
    val now = System.nanoTime()
    if (proxyAddresses == null || (lastUpdate + updateInterval) <= now) {
      this.synchronized {
        if (proxyAddresses == null || (lastUpdate + updateInterval) <= now) {
          val proxyAddressesBuilder = Set.newBuilder[String]
          proxyHosts.foreach { proxyHost =>
            try {
              InetAddress.getAllByName(proxyHost).foreach { proxyAddr =>
                logDebug(s"proxy address is: ${proxyAddr.getHostAddress}")
                proxyAddressesBuilder += proxyAddr.getHostAddress
              }
              lastUpdate = now
            } catch {
              case e: UnknownHostException =>
                logWarning(s"Could not locate $proxyHost - skipping", e)
            }
          }
          proxyAddresses = proxyAddressesBuilder.result()
          if (proxyAddresses.isEmpty) {
            throw new ServletException("Could not locate any of the proxy hosts")
          }
        }
      }
    }
    proxyAddresses
  }

  @throws[IOException]
  @throws[ServletException]
  override def doFilter(req: ServletRequest, resp: ServletResponse, chain: FilterChain): Unit = {
    rejectNonHttpRequests(req)
    val httpReq = req.asInstanceOf[HttpServletRequest]
    val httpResp = resp.asInstanceOf[HttpServletResponse]
    logDebug(s"Remote address for request is: ${httpReq.getRemoteAddr}")
    if (!getProxyAddresses.contains(httpReq.getRemoteAddr)) {
      val redirect = new StringBuilder(findRedirectUrl)
      redirect.append(httpReq.getRequestURI)
      var insertPoint = redirect.indexOf(PROXY_PATH)
      if (insertPoint >= 0) {
        // Add /redirect as the second component of the path so that the RM web
        // proxy knows that this request was a redirect.
        insertPoint += PROXY_PATH.length
        redirect.insert(insertPoint, "/redirect")
      }
      // add the query parameters on the redirect if there were any
      Option(httpReq.getQueryString).filterNot(StringUtils.isEmpty).foreach { queryString =>
        redirect.append("?").append(queryString)
      }
      sendRedirect(httpReq, httpResp, redirect.toString)
    } else {
      Option(httpReq.getCookies).flatMap { cookies =>
        cookies.find { c => c.getName == PROXY_USER_COOKIE_NAME }.map(_.getValue)
      } match {
        case Some(user) =>
          val principal = new AmIpPrincipal(user)
          val requestWrapper = new AmIpServletRequestWrapper(httpReq, principal)
          chain.doFilter(requestWrapper, resp)
        case None =>
          logDebug(s"Could not find $PROXY_USER_COOKIE_NAME cookie, so user will not be set")
          chain.doFilter(req, resp)
      }
    }
  }

  @throws[ServletException]
  private[spark] def findRedirectUrl: String = {
    val addr = if (proxyUriBases.size == 1) {
      // external proxy or not RM HA
      Some(proxyUriBases.values.iterator.next())
    } else if (rmUrls != null) {
      rmUrls.map(url => proxyUriBases(url)).find { host => isValidUrl(host) }
    } else {
      None
    }
    addr.getOrElse {
      throw new ServletException("Could not determine the proxy server for redirection")
    }
  }

  private def isValidUrl(url: String): Boolean = {
    var isValid: Boolean = false
    try {
      val conn = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
      conn.connect()
      isValid = conn.getResponseCode == HttpURLConnection.HTTP_OK
      // If security is enabled, any valid RM which can give 401 Unauthorized is
      // good enough to access. Since AM doesn't have enough credential, auth
      // cannot be completed and hence 401 is fine in such case.
      if (!isValid && UserGroupInformation.isSecurityEnabled) {
        isValid = conn.getResponseCode == HttpURLConnection.HTTP_UNAUTHORIZED ||
          conn.getResponseCode == HttpURLConnection.HTTP_FORBIDDEN
        return isValid
      }
    } catch {
      case e: Exception =>
        logWarning(s"Failed to connect to $url", e)
    }
    isValid
  }

  /**
   * Handle redirects with a status code that can in future support verbs other
   * than GET, thus supporting full REST functionality.
   * <p>
   * The target URL is included in the redirect text returned
   * <p>
   * At the end of this method, the output stream is closed.
   *
   * @param req    request (hence: the verb and any other information
   *                 relevant to a redirect)
   * @param resp   the response
   * @param target the target URL -unencoded
   *
   */
  @throws[IOException]
  private def sendRedirect(
      req: HttpServletRequest, resp: HttpServletResponse, target: String): Unit = {
    logDebug(s"Redirecting ${req.getMethod} ${req.getRequestURI} to $target")
    val location = resp.encodeRedirectURL(target)
    resp.setStatus(HttpServletResponse.SC_FOUND)
    resp.setHeader(LOCATION, location)
    resp.setContentType("text/html")
    val content = s"""
      |<html>
      |<head>
      |  <title>Moved</title>
      |</head>
      |<body>
      |  <h1>Moved</h1>
      |  <div>Content has moved <a href="$location">here</a></div>
      |</body>
      |</html>
      """.stripMargin

    val writer = resp.getWriter
    writer.write(content)
    writer.close()
  }

  /**
   * Reject any request that isn't from an HTTP servlet
   *
   * @param req request
   * @throws ServletException if the request is of the wrong type
   */
  @throws[ServletException]
  private def rejectNonHttpRequests(req: ServletRequest): Unit = {
    if (!req.isInstanceOf[HttpServletRequest]) throw new ServletException(E_HTTP_HTTPS_ONLY)
  }
}

private[spark] object YarnAMIpFilter {
  // YARN-1811: Maintain for backwards compatibility
  @deprecated val PROXY_HOST = "PROXY_HOST"
  @deprecated val PROXY_URI_BASE = "PROXY_URI_BASE"
  val PROXY_HOSTS = "PROXY_HOSTS"
  val PROXY_HOSTS_DELIMITER = ","
  val PROXY_URI_BASES = "PROXY_URI_BASES"
  val PROXY_URI_BASES_DELIMITER = ","
  val PROXY_PATH = "/proxy"
  val PROXY_USER_COOKIE_NAME = "proxy-user"
  val RM_HA_URLS = "RM_HA_URLS"
  val RM_HA_URLS_DELIMITER = ","
  val E_HTTP_HTTPS_ONLY = "This filter only works for HTTP/HTTPS"
  val LOCATION = "Location"
  // update the proxy IP list about every 5 min
  var updateInterval = TimeUnit.MINUTES.toNanos(5)

  // only for testing
  def setUpdateInterval(ns: Long): Unit = updateInterval = ns

  private[spark] class AmIpPrincipal(name: String) extends Principal {
    override def getName: String = name
  }

  private[spark] class AmIpServletRequestWrapper(
      httpReq: HttpServletRequest,
      principal: AmIpPrincipal) extends HttpServletRequestWrapper(httpReq) {
    override def getUserPrincipal: Principal = principal
    override def getRemoteUser: String = principal.getName
    override def isUserInRole(role: String): Boolean = false
  }
}
