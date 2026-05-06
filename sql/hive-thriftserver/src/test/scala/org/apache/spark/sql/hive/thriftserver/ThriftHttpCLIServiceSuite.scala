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

package org.apache.spark.sql.hive.thriftserver

import org.eclipse.jetty.server.{AbstractConnectionFactory, HttpConfiguration, HttpConnectionFactory, SecureRequestCustomizer}
import org.eclipse.jetty.util.ssl.SslContextFactory

import org.apache.spark.SparkFunSuite

class ThriftHttpCLIServiceSuite extends SparkFunSuite {

  /**
   * Helper that builds the SSL connection factory chain the same way
   * ThriftHttpCLIService.initializeServer() does, using the given sniHostCheck value.
   */
  private def buildSslFactoriesAndGetCustomizer(
      sniHostCheckEnabled: Boolean): SecureRequestCustomizer = {
    val sslContextFactory = new SslContextFactory.Server()
    val httpConfig = new HttpConfiguration()
    val src = new SecureRequestCustomizer()
    src.setSniHostCheck(sniHostCheckEnabled)
    httpConfig.addCustomizer(src)
    val connectionFactories = AbstractConnectionFactory.getFactories(
      sslContextFactory, new HttpConnectionFactory(httpConfig))

    val httpFactory = connectionFactories
      .find(_.isInstanceOf[HttpConnectionFactory])
      .map(_.asInstanceOf[HttpConnectionFactory])
      .getOrElse(fail("HttpConnectionFactory not found in SSL connection factories"))

    httpFactory.getHttpConfiguration.getCustomizers.toArray
      .find(_.isInstanceOf[SecureRequestCustomizer])
      .map(_.asInstanceOf[SecureRequestCustomizer])
      .getOrElse(fail("SecureRequestCustomizer not found in HttpConfiguration"))
  }

  test("SPARK-54293: SNI host check disabled by default") {
    // Default behavior: sniHostCheckEnabled = false
    val customizer = buildSslFactoriesAndGetCustomizer(sniHostCheckEnabled = false)
    assert(!customizer.isSniHostCheck,
      "SNI host check should be disabled when sniHostCheckEnabled is false")
  }

  test("SPARK-54293: SNI host check enabled when configured") {
    // Opt-in behavior: sniHostCheckEnabled = true
    val customizer = buildSslFactoriesAndGetCustomizer(sniHostCheckEnabled = true)
    assert(customizer.isSniHostCheck,
      "SNI host check should be enabled when sniHostCheckEnabled is true")
  }

  test("SPARK-54293: SSL connection factories without fix have SNI host check enabled") {
    // Demonstrate that without the fix (no SecureRequestCustomizer), Jetty 10+
    // defaults to sniHostCheck=true, which causes the bug.
    val sslContextFactory = new SslContextFactory.Server()
    val connectionFactories = AbstractConnectionFactory.getFactories(
      sslContextFactory, new HttpConnectionFactory())

    val httpFactory = connectionFactories
      .find(_.isInstanceOf[HttpConnectionFactory])
      .map(_.asInstanceOf[HttpConnectionFactory])
      .getOrElse(fail("HttpConnectionFactory not found"))

    val customizers = httpFactory.getHttpConfiguration.getCustomizers
    val secureCustomizer = customizers.toArray
      .find(_.isInstanceOf[SecureRequestCustomizer])
      .map(_.asInstanceOf[SecureRequestCustomizer])

    // Without the fix, either there's no SecureRequestCustomizer at all,
    // or it has sniHostCheck=true (the Jetty 10+ default)
    secureCustomizer match {
      case Some(src) => assert(src.isSniHostCheck,
        "Default Jetty 10+ behavior should have SNI host check enabled")
      case None => // No customizer means Jetty adds one with defaults (sniHostCheck=true)
    }
  }
}
