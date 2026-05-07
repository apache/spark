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

import javax.crypto.SecretKey

import io.jsonwebtoken.{JwtException, Jwts}
import io.jsonwebtoken.io.Decoders
import io.jsonwebtoken.security.Keys
import jakarta.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}
import jakarta.servlet.http.{HttpServletRequest, HttpServletResponse}

/**
 * A servlet filter that requires JWS, a cryptographically signed JSON Web Token, in the header.
 *
 * Like the other UI filters, the following configurations are required to use this filter.
 * {{{
 *   - spark.ui.filters=org.apache.spark.ui.JWSFilter
 *   - spark.org.apache.spark.ui.JWSFilter.param.secretKey=BASE64URL-ENCODED-YOUR-PROVIDED-KEY
 * }}}
 * The HTTP request should have {@code Authorization: Bearer <jws>} header.
 * {{{
 *   - <jws> is a string with three fields, '<header>.<payload>.<signature>'.
 *   - <header> is supposed to be a base64url-encoded string of '{"alg":"HS256","typ":"JWT"}'.
 *   - <payload> is a base64url-encoded string of fully-user-defined content.
 *   - <signature> is a signature based on '<header>.<payload>' and a user-provided key parameter.
 * }}}
 */
private class JWSFilter extends Filter {
  private val AUTHORIZATION = "Authorization"

  private var key: SecretKey = null

  /**
   * Load and validate the configurtions:
   * - IllegalArgumentException will happen if the user didn't provide this argument
   * - WeakKeyException will happen if the user-provided value is insufficient
   */
  override def init(config: FilterConfig): Unit = {
    key = Keys.hmacShaKeyFor(Decoders.BASE64URL.decode(config.getInitParameter("secretKey")));
  }

  override def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain): Unit = {
    val hreq = req.asInstanceOf[HttpServletRequest]
    val hres = res.asInstanceOf[HttpServletResponse]
    hres.setHeader("Cache-Control", "no-cache, no-store, must-revalidate")

    try {
      val header = hreq.getHeader(AUTHORIZATION)
      header match {
        case null =>
          hres.sendError(HttpServletResponse.SC_FORBIDDEN, s"${AUTHORIZATION} header is missing.")
        case s"Bearer $token" =>
          val claims = Jwts.parser().verifyWith(key).build().parseSignedClaims(token)
          chain.doFilter(req, res)
        case _ =>
          hres.sendError(HttpServletResponse.SC_FORBIDDEN, s"Malformed ${AUTHORIZATION} header.")
      }
    } catch {
      case e: JwtException =>
        // We intentionally don't expose the detail of JwtException here
        hres.sendError(HttpServletResponse.SC_FORBIDDEN, "JWT Validate Fail")
    }
  }
}
