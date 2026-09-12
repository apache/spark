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

import jakarta.servlet.{FilterChain, ServletRequest, ServletResponse}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.logging.log4j.Level
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.History._
import org.apache.spark.util.Utils

class HistoryServerAccessLogFilterSuite extends SparkFunSuite with MockitoSugar {

  test("logs History Server access records") {
    val filter = new HistoryServerAccessLogFilter(new SparkConf(false))
    val request = mockRequest(
      uri = "/api/v1/applications",
      queryString = "token=visible&session=secret-value&status=complete",
      userAgent = "curl 8.0",
      referer = "http://localhost/history")
    when(request.getRemoteUser).thenReturn("alice")
    val response = mock[HttpServletResponse]
    val chain = new FilterChain {
      override def doFilter(req: ServletRequest, res: ServletResponse): Unit = {
        res.asInstanceOf[HttpServletResponse].setStatus(HttpServletResponse.SC_CREATED)
      }
    }

    val message = withAccessLogAppender {
      filter.doFilter(request, response, chain)
    }.head

    assert(message.contains("Spark History Server access"))
    assert(message.contains("method=GET"))
    assert(message.contains("uri=/api/v1/applications"))
    assert(message.contains(s"token=${Utils.REDACTION_REPLACEMENT_TEXT}"))
    assert(message.contains(s"session=${Utils.REDACTION_REPLACEMENT_TEXT}"))
    assert(message.contains("status=complete"))
    assert(message.contains("status=201"))
    assert(message.contains("remoteAddress=192.0.2.10"))
    assert(message.contains("user=alice"))
    assert(message.contains("userAgent=curl_8.0"))
    assert(message.contains("referer=http://localhost/history"))
    assert(message.contains("error=-"))
  }

  test("does not log excluded paths") {
    val filter = new HistoryServerAccessLogFilter(new SparkConf(false))
    val response = mock[HttpServletResponse]
    var calls = 0
    val chain = new FilterChain {
      override def doFilter(req: ServletRequest, res: ServletResponse): Unit = {
        calls += 1
      }
    }

    val messages = withAccessLogAppender {
      filter.doFilter(mockRequest(uri = "/favicon.ico"), response, chain)
      filter.doFilter(mockRequest(uri = "/static/bootstrap.css"), response, chain)
    }

    assert(messages.isEmpty)
    assert(calls === 2)
  }

  test("redacts query parameters without values") {
    val filter = new HistoryServerAccessLogFilter(new SparkConf(false))
    val request = mockRequest(
      uri = "/api/v1/applications",
      queryString = "password&plain")
    val response = mock[HttpServletResponse]
    val chain = new FilterChain {
      override def doFilter(req: ServletRequest, res: ServletResponse): Unit = {}
    }

    val message = withAccessLogAppender {
      filter.doFilter(request, response, chain)
    }.head

    assert(message.contains(s"query=${Utils.REDACTION_REPLACEMENT_TEXT}&plain"))
  }


  test("logs exception class and server error status") {
    val filter = new HistoryServerAccessLogFilter(new SparkConf(false))
    val request = mockRequest(uri = "/history/app-1/jobs")
    val response = mock[HttpServletResponse]
    val chain = new FilterChain {
      override def doFilter(req: ServletRequest, res: ServletResponse): Unit = {
        throw new IllegalStateException("failed")
      }
    }

    val messages = withAccessLogAppender {
      intercept[IllegalStateException] {
        filter.doFilter(request, response, chain)
      }
    }

    assert(messages.size === 1)
    assert(messages.head.contains("status=500"))
    assert(messages.head.contains("error=java.lang.IllegalStateException"))
  }

  test("uses configured excluded path prefixes") {
    val conf = new SparkConf(false)
      .set(HISTORY_SERVER_UI_ACCESS_LOG_EXCLUDE_PATHS, Seq("/api/private"))
    val filter = new HistoryServerAccessLogFilter(conf)
    val response = mock[HttpServletResponse]
    val chain = new FilterChain {
      override def doFilter(req: ServletRequest, res: ServletResponse): Unit = {}
    }

    val messages = withAccessLogAppender {
      filter.doFilter(mockRequest(uri = "/api/private/health"), response, chain)
      filter.doFilter(mockRequest(uri = "/favicon.ico"), response, chain)
    }

    assert(messages.size === 1)
    assert(messages.head.contains("uri=/favicon.ico"))
  }

  private def mockRequest(
      uri: String,
      queryString: String = null,
      userAgent: String = "JUnit",
      referer: String = null): HttpServletRequest = {
    val request = mock[HttpServletRequest]
    when(request.getMethod).thenReturn("GET")
    when(request.getRequestURI).thenReturn(uri)
    when(request.getQueryString).thenReturn(queryString)
    when(request.getRemoteAddr).thenReturn("192.0.2.10")
    when(request.getRemoteUser).thenReturn(null)
    when(request.getUserPrincipal).thenReturn(null)
    when(request.getHeader("User-Agent")).thenReturn(userAgent)
    when(request.getHeader("Referer")).thenReturn(referer)
    request
  }

  private def withAccessLogAppender(f: => Unit): Seq[String] = {
    val logAppender = new LogAppender
    withLogAppender(
        logAppender,
        loggerNames = Seq(classOf[HistoryServerAccessLogFilter].getName),
        level = Some(Level.INFO)) {
      f
    }
    logAppender.loggingEvents.map(_.getMessage.getFormattedMessage)
      .filter(_.contains("Spark History Server access"))
      .toSeq
  }
}
