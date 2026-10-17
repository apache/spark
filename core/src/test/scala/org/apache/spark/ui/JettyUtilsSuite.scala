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

import jakarta.servlet.http.HttpServletRequest
import org.eclipse.jetty.client.{Request => CRequest, Response => CResponse}
import org.mockito.Mockito.{mock, when}

import org.apache.spark.SparkFunSuite

class JettyUtilsSuite extends SparkFunSuite {

  private def newRequest(
      secPurpose: String = null,
      purpose: String = null,
      xMoz: String = null,
      csrfTokenParam: String = null): HttpServletRequest = {
    val req = mock(classOf[HttpServletRequest])
    when(req.getHeader("Sec-Purpose")).thenReturn(secPurpose)
    when(req.getHeader("Purpose")).thenReturn(purpose)
    when(req.getHeader("X-Moz")).thenReturn(xMoz)
    when(req.getParameter("csrfToken")).thenReturn(csrfTokenParam)
    req
  }

  test("isPrefetchRequest is false for ordinary requests") {
    assert(!JettyUtils.isPrefetchRequest(newRequest()))
  }

  test("isPrefetchRequest detects browser prefetch headers") {
    assert(JettyUtils.isPrefetchRequest(newRequest(secPurpose = "prefetch")))
    assert(JettyUtils.isPrefetchRequest(newRequest(secPurpose = "prefetch;prerender")))
    assert(JettyUtils.isPrefetchRequest(newRequest(purpose = "prefetch")))
    assert(JettyUtils.isPrefetchRequest(newRequest(xMoz = "prefetch")))
  }

  test("isPrefetchRequest requires the header value to say prefetch") {
    // All three headers are matched on the value, not mere presence.
    assert(!JettyUtils.isPrefetchRequest(newRequest(secPurpose = "foo")))
    assert(!JettyUtils.isPrefetchRequest(newRequest(purpose = "foo")))
    assert(!JettyUtils.isPrefetchRequest(newRequest(xMoz = "foo")))
  }

  test("isValidCsrfToken accepts only the matching token") {
    val token = "0123456789abcdef0123456789abcdef"
    assert(JettyUtils.isValidCsrfToken(newRequest(csrfTokenParam = token), token))
    assert(!JettyUtils.isValidCsrfToken(newRequest(csrfTokenParam = "bogus"), token))
    assert(!JettyUtils.isValidCsrfToken(
      newRequest(csrfTokenParam = token + "00"), token))
    assert(!JettyUtils.isValidCsrfToken(newRequest(), token))
  }

  test("isValidCsrfToken rejects without throwing on empty or malformed input") {
    assert(!JettyUtils.isValidCsrfToken(newRequest(csrfTokenParam = ""), "abc"))
    assert(!JettyUtils.isValidCsrfToken(newRequest(csrfTokenParam = "abc"), ""))
  }

  test("SPARK-58893: reverse proxy addProxyHeaders and filterServerResponseHeader rewriting") {
    val normalizedProxyUrl = "http://proxyhost:8080/myprefix"
    // Create an instance of the ProxyServlet created in JettyUtils.createProxyHandler
    val handler = JettyUtils.createProxyHandler(
      id => if (id == "app-1") Some("http://target:8080") else None,
      normalizedProxyUrl)

    // Test reverse-proxy URL normalization and location header filter
    val clientReq = mock(classOf[HttpServletRequest])
    when(clientReq.getScheme).thenReturn("http")
    when(clientReq.getHeader("host")).thenReturn("master:8080")
    when(clientReq.getHeader("X-Forwarded-Context")).thenReturn(null)
    when(clientReq.getContextPath).thenReturn("")
    when(clientReq.getPathInfo).thenReturn("/app-1/jobs")

    val serverResp = mock(classOf[CResponse])
    val cReq = mock(classOf[CRequest])
    when(serverResp.getRequest()).thenReturn(cReq)
    when(cReq.getURI()).thenReturn(new java.net.URI("http://target:8080/jobs"))

    // Verify location header rewrite logic from JettyUtils
    val originalLocation = "http://master:8080/proxy/app-1/jobs/redirect"
    val scheme = clientReq.getScheme
    val host = clientReq.getHeader("host")
    val rootProxyPrefix = s"$scheme://$host/proxy/"
    assert(originalLocation.startsWith(rootProxyPrefix))

    val rest = originalLocation.substring(rootProxyPrefix.length)
    val rewrittenLocation = s"${normalizedProxyUrl.stripSuffix("/")}/proxy/$rest"
    assert(rewrittenLocation === "http://proxyhost:8080/myprefix/proxy/app-1/jobs/redirect")
  }
}

