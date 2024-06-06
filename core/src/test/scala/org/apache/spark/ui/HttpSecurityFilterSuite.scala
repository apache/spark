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

import java.util.UUID

import scala.jdk.CollectionConverters._

import jakarta.servlet.FilterChain
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, times, verify, when}

import org.apache.spark._
import org.apache.spark.internal.config.UI._

class HttpSecurityFilterSuite extends SparkFunSuite {

  test("filter bad user input") {
    val badValues = Map(
      "encoded" -> "Encoding:base64%0d%0a%0d%0aPGh0bWw%2bjcmlwdD48L2h0bWw%2b",
      "alert1" -> """>"'><script>alert(401)<%2Fscript>""",
      "alert2" -> """app-20161208133404-0002<iframe+src%3Djavascript%3Aalert(1705)>""",
      "alert3" -> """stdout'%2Balert(60)%2B'""",
      "html" -> """stdout'"><iframe+id%3D1131+src%3Dhttp%3A%2F%2Fdemo.test.net%2Fphishing.html>"""
    )
    val badKeys = badValues.map(_.swap)
    val goodInput = Map("goodKey" -> "goodValue")

    val conf = new SparkConf()
    val filter = new HttpSecurityFilter(conf, new SecurityManager(conf))

    def doRequest(k: String, v: String): HttpServletRequest = {
      val req = mockRequest(params = Map(k -> Array(v)))
      val chain = mock(classOf[FilterChain])
      val res = mock(classOf[HttpServletResponse])
      filter.doFilter(req, res, chain)

      val captor = ArgumentCaptor.forClass(classOf[HttpServletRequest])
      verify(chain).doFilter(captor.capture(), any())
      captor.getValue()
    }

    badKeys.foreach { case (k, v) =>
      val req = doRequest(k, v)
      assert(req.getParameter(k) === null)
      assert(req.getParameterValues(k) === null)
      assert(!req.getParameterMap().containsKey(k))
    }

    badValues.foreach { case (k, v) =>
      val req = doRequest(k, v)
      assert(req.getParameter(k) !== null)
      assert(req.getParameter(k) !== v)
      assert(req.getParameterValues(k) !== null)
      assert(req.getParameterValues(k) !== Array(v))
      assert(req.getParameterMap().get(k) !== null)
      assert(req.getParameterMap().get(k) !== Array(v))
    }

    goodInput.foreach { case (k, v) =>
      val req = doRequest(k, v)
      assert(req.getParameter(k) === v)
      assert(req.getParameterValues(k) === Array(v))
      assert(req.getParameterMap().get(k) === Array(v))
    }
  }

  test("perform access control") {
    val conf = new SparkConf(false)
      .set(ACLS_ENABLE, true)
      .set(ADMIN_ACLS, Seq("admin"))
      .set(UI_VIEW_ACLS, Seq("alice"))
    val secMgr = new SecurityManager(conf)

    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)

    when(req.getRemoteUser()).thenReturn("admin")
    filter.doFilter(req, res, chain)
    verify(chain, times(1)).doFilter(any(), any())

    when(req.getRemoteUser()).thenReturn("alice")
    filter.doFilter(req, res, chain)
    verify(chain, times(2)).doFilter(any(), any())

    // Because the current user is added to the view ACLs, let's try to create an invalid
    // name, to avoid matching some common user name.
    when(req.getRemoteUser()).thenReturn(UUID.randomUUID().toString())
    filter.doFilter(req, res, chain)

    // chain.doFilter() should not be called again, so same count as above.
    verify(chain, times(2)).doFilter(any(), any())
    verify(res).sendError(meq(HttpServletResponse.SC_FORBIDDEN), any())
  }

  test("set security-related headers") {
    val conf = new SparkConf(false)
      .set(UI_ALLOW_FRAMING_FROM, "example.com")
      .set(UI_X_XSS_PROTECTION, "xssProtection")
      .set(UI_X_CONTENT_TYPE_OPTIONS, true)
      .set(UI_STRICT_TRANSPORT_SECURITY, "tsec")
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    when(req.getScheme()).thenReturn("https")

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    Map(
      "X-Frame-Options" -> "ALLOW-FROM example.com",
      "X-XSS-Protection" -> "xssProtection",
      "X-Content-Type-Options" -> "nosniff",
      "Strict-Transport-Security" -> "tsec"
    ).foreach { case (name, value) =>
      verify(res).setHeader(meq(name), meq(value))
    }
  }

  test("doAs impersonation") {
    val conf = new SparkConf(false)
      .set(ACLS_ENABLE, true)
      .set(ADMIN_ACLS, Seq("admin"))
      .set(UI_VIEW_ACLS, Seq("proxy"))

    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])
    val filter = new HttpSecurityFilter(conf, secMgr)

    // First try with a non-admin so that the admin check is verified. This ensures that
    // the admin check is setting the expected error, since the impersonated user would
    // have permissions to process the request.
    when(req.getParameter("doAs")).thenReturn("proxy")
    when(req.getRemoteUser()).thenReturn("bob")
    filter.doFilter(req, res, chain)
    verify(res, times(1)).sendError(meq(HttpServletResponse.SC_FORBIDDEN), any())

    when(req.getRemoteUser()).thenReturn("admin")
    filter.doFilter(req, res, chain)
    verify(chain, times(1)).doFilter(any(), any())

    // Check that impersonation was actually performed by checking the wrapped request.
    val captor = ArgumentCaptor.forClass(classOf[HttpServletRequest])
    verify(chain).doFilter(captor.capture(), any())
    val wrapped = captor.getValue()
    assert(wrapped.getRemoteUser() === "proxy")

    // Impersonating a user without view permissions should cause an error.
    when(req.getParameter("doAs")).thenReturn("alice")
    filter.doFilter(req, res, chain)
    verify(res, times(2)).sendError(meq(HttpServletResponse.SC_FORBIDDEN), any())
  }

  private def mockRequest(params: Map[String, Array[String]] = Map()): HttpServletRequest = {
    val req = mock(classOf[HttpServletRequest])
    when(req.getParameterMap()).thenReturn(params.asJava)
    req
  }

}
