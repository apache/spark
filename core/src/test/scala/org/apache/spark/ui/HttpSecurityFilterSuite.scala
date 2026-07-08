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

import jakarta.servlet.{FilterChain, ServletRequest, ServletResponse}
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
      .set(UI_CONTENT_SECURITY_POLICY_ENABLED, true)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    when(req.getScheme()).thenReturn("https")

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    // CSP header contains a dynamic nonce and frame-ancestors with the configured URI
    val cspCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(res).setHeader(meq("Content-Security-Policy"), cspCaptor.capture())
    val cspValue = cspCaptor.getValue
    assert(cspValue.startsWith("default-src 'self'; script-src 'self' 'nonce-"))
    assert(cspValue.contains("style-src 'self' 'unsafe-inline'"))
    assert(cspValue.contains("img-src 'self' data:"))
    assert(cspValue.contains("object-src 'none'"))
    assert(cspValue.contains("base-uri 'self'"))
    assert(cspValue.contains("frame-ancestors 'self' example.com"))

    // X-Frame-Options is always SAMEORIGIN as a fallback for legacy browsers.
    // The allowFramingFrom config is honored via CSP frame-ancestors instead.
    Map(
      "X-Frame-Options" -> "SAMEORIGIN",
      "X-XSS-Protection" -> "xssProtection",
      "X-Content-Type-Options" -> "nosniff",
      "Strict-Transport-Security" -> "tsec"
    ).foreach { case (name, value) =>
      verify(res).setHeader(meq(name), meq(value))
    }
  }

  test("no CSP header when CSP is disabled regardless of frameAncestors setting") {
    val conf = new SparkConf(false)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    // CSP is disabled by default, so no CSP header should be emitted
    verify(res, times(0)).setHeader(meq("Content-Security-Policy"), any())
  }

  test("X-XSS-Protection defaults to 0") {
    val conf = new SparkConf(false)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    verify(res).setHeader(meq("X-XSS-Protection"), meq("0"))
  }

  test("frame-ancestors is included in CSP when both CSP and frameAncestors are enabled") {
    val conf = new SparkConf(false)
      .set(UI_CONTENT_SECURITY_POLICY_ENABLED, true)
      .set(UI_ALLOW_FRAMING_FROM, "https://example.com")
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    val cspCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(res).setHeader(meq("Content-Security-Policy"), cspCaptor.capture())
    assert(cspCaptor.getValue.contains("frame-ancestors 'self' https://example.com"))
  }

  test("frame-ancestors is excluded from CSP when frameAncestors is disabled") {
    val conf = new SparkConf(false)
      .set(UI_CONTENT_SECURITY_POLICY_ENABLED, true)
      .set(UI_CONTENT_SECURITY_POLICY_FRAME_ANCESTORS_ENABLED, false)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    val cspCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(res).setHeader(meq("Content-Security-Policy"), cspCaptor.capture())
    assert(!cspCaptor.getValue.contains("frame-ancestors"))
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

  test("CSP nonce is available during chain.doFilter and cleared after") {
    val conf = new SparkConf(false).set(UI_CONTENT_SECURITY_POLICY_ENABLED, true)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])

    var nonceInsideChain: String = null
    val chain = new FilterChain {
      override def doFilter(req: ServletRequest, res: ServletResponse): Unit = {
        nonceInsideChain = CspNonce.get
      }
    }

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    // Nonce should have been available inside chain.doFilter
    assert(nonceInsideChain != null && nonceInsideChain.nonEmpty)
    // Nonce should be cleared after doFilter completes
    assert(CspNonce.get === null)

    // CSP header should contain the same nonce that was available inside the chain
    val cspCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(res).setHeader(meq("Content-Security-Policy"), cspCaptor.capture())
    assert(cspCaptor.getValue.contains(s"'nonce-$nonceInsideChain'"))
  }

  test("CSP nonce is cleared even when access is denied") {
    val conf = new SparkConf(false)
      .set(ACLS_ENABLE, true)
      .set(UI_VIEW_ACLS, Seq("alice"))
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    when(req.getRemoteUser()).thenReturn("unauthorized-user")
    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    // chain.doFilter should not have been called
    verify(chain, times(0)).doFilter(any(), any())
    // Nonce should still be cleared
    assert(CspNonce.get === null)
  }

  test("each request gets a unique CSP nonce") {
    val conf = new SparkConf(false)
    val secMgr = new SecurityManager(conf)
    val filter = new HttpSecurityFilter(conf, secMgr)

    val nonces = (1 to 3).map { _ =>
      val req = mockRequest()
      val res = mock(classOf[HttpServletResponse])
      var nonce: String = null
      val chain = new FilterChain {
        override def doFilter(req: ServletRequest, res: ServletResponse): Unit = {
          nonce = CspNonce.get
        }
      }
      filter.doFilter(req, res, chain)
      nonce
    }

    // All nonces should be unique
    assert(nonces.distinct.size === 3)
  }

  test("allowFramingFrom value is sanitized to prevent CSP directive injection") {
    val conf = new SparkConf(false)
      .set(UI_ALLOW_FRAMING_FROM, "evil.com; script-src 'unsafe-inline'")
      .set(UI_CONTENT_SECURITY_POLICY_ENABLED, true)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    val cspCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(res).setHeader(meq("Content-Security-Policy"), cspCaptor.capture())
    val cspValue = cspCaptor.getValue
    // Semicolons should be stripped, preventing directive injection
    assert(!cspValue.contains("script-src 'unsafe-inline'"))
    assert(cspValue.contains("frame-ancestors 'self' evil.com"))
  }

  test("X-Frame-Options is set even when access is denied") {
    val conf = new SparkConf(false)
      .set(ACLS_ENABLE, true)
      .set(UI_VIEW_ACLS, Seq("alice"))
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    when(req.getRemoteUser()).thenReturn("unauthorized-user")
    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    // chain.doFilter should not have been called
    verify(chain, times(0)).doFilter(any(), any())
    // But X-Frame-Options should still be set
    verify(res).setHeader(meq("X-Frame-Options"), meq("SAMEORIGIN"))
  }

  test("allowFramingFrom=DENY maps to frame-ancestors 'none'") {
    val conf = new SparkConf(false)
      .set(UI_ALLOW_FRAMING_FROM, "DENY")
      .set(UI_CONTENT_SECURITY_POLICY_ENABLED, true)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    val cspCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(res).setHeader(meq("Content-Security-Policy"), cspCaptor.capture())
    assert(cspCaptor.getValue.contains("frame-ancestors 'none'"))
  }

  test("allowFramingFrom=SAMEORIGIN maps to frame-ancestors 'self'") {
    val conf = new SparkConf(false)
      .set(UI_ALLOW_FRAMING_FROM, "SAMEORIGIN")
      .set(UI_CONTENT_SECURITY_POLICY_ENABLED, true)
    val secMgr = new SecurityManager(conf)
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new HttpSecurityFilter(conf, secMgr)
    filter.doFilter(req, res, chain)

    val cspCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(res).setHeader(meq("Content-Security-Policy"), cspCaptor.capture())
    assert(cspCaptor.getValue.contains("frame-ancestors 'self'"))
    assert(!cspCaptor.getValue.contains("SAMEORIGIN"))
  }

  private def mockRequest(params: Map[String, Array[String]] = Map()): HttpServletRequest = {
    val req = mock(classOf[HttpServletRequest])
    when(req.getParameterMap()).thenReturn(params.asJava)
    req
  }

}
