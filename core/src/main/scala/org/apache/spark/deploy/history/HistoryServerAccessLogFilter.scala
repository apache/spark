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

package org.apache.spark.deploy.history

import java.net.URLDecoder
import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import jakarta.servlet.{Filter, FilterChain, ServletRequest, ServletResponse}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse, HttpServletResponseWrapper}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{History, SECRET_REDACTION_PATTERN}
import org.apache.spark.util.Utils

private[history] class HistoryServerAccessLogFilter(conf: SparkConf)
  extends Filter with Logging {

  import HistoryServerAccessLogFilter._

  private val redactionPattern = Some(conf.get(SECRET_REDACTION_PATTERN))

  private val excludedPathPrefixes = conf.get(History.HISTORY_SERVER_UI_ACCESS_LOG_EXCLUDE_PATHS)
    .map(normalizePathPrefix)
    .filter(_.nonEmpty)

  override def doFilter(
      request: ServletRequest,
      response: ServletResponse,
      chain: FilterChain): Unit = {
    (request, response) match {
      case (httpRequest: HttpServletRequest, httpResponse: HttpServletResponse)
          if !shouldSkip(httpRequest) =>
        doFilterWithAccessLog(httpRequest, httpResponse, chain)

      case _ =>
        chain.doFilter(request, response)
    }
  }

  private def doFilterWithAccessLog(
      request: HttpServletRequest,
      response: HttpServletResponse,
      chain: FilterChain): Unit = {
    val responseWrapper = new StatusCaptureResponse(response)
    val startNs = System.nanoTime()
    var error: Throwable = null
    try {
      chain.doFilter(request, responseWrapper)
    } catch {
      case NonFatal(e) =>
        error = e
        throw e
    } finally {
      logAccess(request, responseWrapper, startNs, Option(error))
    }
  }

  private def shouldSkip(request: HttpServletRequest): Boolean = {
    val requestPath = Option(request.getRequestURI).getOrElse("")
    excludedPathPrefixes.exists(matchesPathPrefix(requestPath, _))
  }

  private def logAccess(
      request: HttpServletRequest,
      response: StatusCaptureResponse,
      startNs: Long,
      error: Option[Throwable]): Unit = {
    val durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs)
    val status = if (error.isDefined && response.status == HttpServletResponse.SC_OK) {
      HttpServletResponse.SC_INTERNAL_SERVER_ERROR
    } else {
      response.status
    }
    val errorClass = error.map(_.getClass.getName).getOrElse(MissingField)

    logInfo(log"Spark History Server access" +
      log" method=${MDC(HTTP_METHOD, field(request.getMethod))}" +
      log" uri=${MDC(URI, field(redact(request.getRequestURI)))}" +
      log" query=${MDC(HTTP_QUERY_STRING, redactQueryString(request.getQueryString))}" +
      log" status=${MDC(HTTP_STATUS_CODE, status)}" +
      log" durationMs=${MDC(DURATION, durationMs)}" +
      log" remoteAddress=${MDC(REMOTE_ADDRESS, field(request.getRemoteAddr))}" +
      log" user=${MDC(USER_NAME, field(remoteUser(request)))}" +
      log" userAgent=${MDC(HTTP_USER_AGENT, field(redact(request.getHeader("User-Agent"))))}" +
      log" referer=${MDC(HTTP_REFERER, field(redact(request.getHeader("Referer"))))}" +
      log" error=${MDC(ERROR, errorClass)}")
  }

  private def remoteUser(request: HttpServletRequest): String = {
    Option(request.getRemoteUser)
      .orElse(Option(request.getUserPrincipal).map(_.getName))
      .orNull
  }

  private def redact(value: String): String = {
    Option(value).map(Utils.redact(redactionPattern, _)).orNull
  }

  private def redactQueryString(queryString: String): String = {
    Option(queryString)
      .filter(_.nonEmpty)
      .map(_.split("&", -1).map(redactQueryParam).mkString("&"))
      .getOrElse(MissingField)
  }

  private def redactQueryParam(param: String): String = {
    val separator = param.indexOf('=')
    if (separator < 0) {
      val decodedParam = decodeQueryComponent(param)
      val redactedParam = Utils.redact(redactionPattern, decodedParam)
      if (redactedParam != decodedParam) {
        Utils.REDACTION_REPLACEMENT_TEXT
      } else {
        field(param)
      }
    } else {
      val rawKey = param.substring(0, separator)
      val rawValue = param.substring(separator + 1)
      val decodedKey = decodeQueryComponent(rawKey)
      val decodedValue = decodeQueryComponent(rawValue)
      val redactedValue = Utils.redact(redactionPattern, Seq(decodedKey -> decodedValue)).head._2
      val valueToLog = if (redactedValue == Utils.REDACTION_REPLACEMENT_TEXT) {
        Utils.REDACTION_REPLACEMENT_TEXT
      } else {
        rawValue
      }
      s"${field(rawKey)}=${field(valueToLog)}"
    }
  }
}

private[history] object HistoryServerAccessLogFilter {

  private val MissingField = "-"

  private def normalizePathPrefix(path: String): String = {
    val trimmed = path.trim
    if (trimmed.isEmpty) {
      ""
    } else {
      val withLeadingSlash = if (trimmed.startsWith("/")) trimmed else s"/$trimmed"
      if (withLeadingSlash == "/") withLeadingSlash else withLeadingSlash.stripSuffix("/")
    }
  }

  private def matchesPathPrefix(requestPath: String, prefix: String): Boolean = {
    prefix == "/" || requestPath == prefix || requestPath.startsWith(s"$prefix/")
  }

  private def decodeQueryComponent(value: String): String = {
    try {
      URLDecoder.decode(value, UTF_8.name())
    } catch {
      case NonFatal(_) => value
    }
  }

  private def field(value: String): String = {
    Option(value)
      .filter(_.nonEmpty)
      .map { v =>
        v.map {
          case c if Character.isWhitespace(c) || Character.isISOControl(c) => '_'
          case c => c
        }.mkString
      }
      .getOrElse(MissingField)
  }

  private class StatusCaptureResponse(response: HttpServletResponse)
    extends HttpServletResponseWrapper(response) {

    private var _status = HttpServletResponse.SC_OK

    def status: Int = _status

    override def setStatus(sc: Int): Unit = {
      _status = sc
      super.setStatus(sc)
    }

    override def sendError(sc: Int): Unit = {
      _status = sc
      super.sendError(sc)
    }

    override def sendError(sc: Int, msg: String): Unit = {
      _status = sc
      super.sendError(sc, msg)
    }

    override def sendRedirect(location: String): Unit = {
      _status = HttpServletResponse.SC_FOUND
      super.sendRedirect(location)
    }
  }
}
