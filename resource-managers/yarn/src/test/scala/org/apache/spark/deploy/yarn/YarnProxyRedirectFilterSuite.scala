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

import java.io.{PrintWriter, StringWriter}
import javax.servlet.FilterChain
import javax.servlet.http.{Cookie, HttpServletRequest, HttpServletResponse}

import org.mockito.Mockito._

import org.apache.spark.SparkFunSuite

class YarnProxyRedirectFilterSuite extends SparkFunSuite {

  test("redirect proxied requests, pass-through others") {
    val requestURL = "http://example.com:1234/foo?"
    val filter = new YarnProxyRedirectFilter()
    val cookies = Array(new Cookie(YarnProxyRedirectFilter.COOKIE_NAME, "dr.who"))

    val req = mock(classOf[HttpServletRequest])

    // First request mocks a YARN proxy request (with the cookie set), second one has no cookies.
    when(req.getCookies()).thenReturn(cookies, null)
    when(req.getRequestURL()).thenReturn(new StringBuffer(requestURL))

    val res = mock(classOf[HttpServletResponse])
    when(res.getWriter()).thenReturn(new PrintWriter(new StringWriter()))

    val chain = mock(classOf[FilterChain])

    // First request is proxied.
    filter.doFilter(req, res, chain)
    verify(chain, never()).doFilter(req, res)

    // Second request is not, so should invoke the filter chain.
    filter.doFilter(req, res, chain)
    verify(chain, times(1)).doFilter(req, res)
  }

}
