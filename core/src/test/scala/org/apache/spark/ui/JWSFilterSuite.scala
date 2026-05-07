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

import java.util.{Base64, HashMap => JHashMap}

import scala.jdk.CollectionConverters._

import jakarta.servlet.{FilterChain, FilterConfig, ServletContext}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{mock, times, verify, when}

import org.apache.spark._

class JWSFilterSuite extends SparkFunSuite {
  // {"alg":"HS256","typ":"JWT"} => eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9, {} => e30
  private val TOKEN =
      "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.e30.4EKWlOkobpaAPR0J4BE0cPQ-ZD1tRQKLZp1vtE7upPw"

  private val TEST_KEY = Base64.getUrlEncoder.encodeToString(
      "Visit https://spark.apache.org to download Apache Spark.".getBytes())

  test("Should fail when a parameter is missing") {
    val filter = new JWSFilter()
    val params = new JHashMap[String, String]
    val m = intercept[IllegalArgumentException] {
      filter.init(new DummyFilterConfig(params))
    }.getMessage()
    assert(m.contains("Decode argument cannot be null"))
  }

  test("Succeed to initialize") {
    val filter = new JWSFilter()
    val params = new JHashMap[String, String]
    params.put("secretKey", TEST_KEY)
    filter.init(new DummyFilterConfig(params))
  }

  test("Should response with SC_FORBIDDEN when it cannot verify JWS") {
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new JWSFilter()
    val params = new JHashMap[String, String]
    params.put("secretKey", TEST_KEY)
    val conf = new DummyFilterConfig(params)
    filter.init(conf)

    // 'Authorization' header is missing
    filter.doFilter(req, res, chain)
    verify(res).sendError(meq(HttpServletResponse.SC_FORBIDDEN),
      meq("Authorization header is missing."))
    verify(chain, times(0)).doFilter(any(), any())

    // The value of Authorization field is not 'Bearer <token>' style.
    when(req.getHeader("Authorization")).thenReturn("Invalid")
    filter.doFilter(req, res, chain)
    verify(res).sendError(meq(HttpServletResponse.SC_FORBIDDEN),
      meq("Malformed Authorization header."))
    verify(chain, times(0)).doFilter(any(), any())
  }

  test("Should succeed on valid JWS") {
    val req = mockRequest()
    val res = mock(classOf[HttpServletResponse])
    val chain = mock(classOf[FilterChain])

    val filter = new JWSFilter()
    val params = new JHashMap[String, String]
    params.put("secretKey", TEST_KEY)
    val conf = new DummyFilterConfig(params)
    filter.init(conf)

    when(req.getHeader("Authorization")).thenReturn(s"Bearer $TOKEN")
    filter.doFilter(req, res, chain)
    verify(chain, times(1)).doFilter(any(), any())
  }

  private def mockRequest(params: Map[String, Array[String]] = Map()): HttpServletRequest = {
    val req = mock(classOf[HttpServletRequest])
    when(req.getParameterMap()).thenReturn(params.asJava)
    req
  }

  class DummyFilterConfig (val map: java.util.Map[String, String]) extends FilterConfig {
    override def getFilterName: String = "dummy"

    override def getInitParameter(arg0: String): String = map.get(arg0)

    override def getInitParameterNames: java.util.Enumeration[String] =
      java.util.Collections.enumeration(map.keySet)

    override def getServletContext: ServletContext = null
  }
}
