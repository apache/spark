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

  test("SPARK-54293: SSL connection factories should disable SNI host check") {
    // Reproduce the connection factory chain created by ThriftHttpCLIService
    // when SSL is enabled. Without the fix, no SecureRequestCustomizer is added,
    // and Jetty 10+ defaults sniHostCheck to true, causing SSL failures when
    // the certificate CN doesn't match the hostname.
    val sslContextFactory = new SslContextFactory.Server()
    val httpConfig = new HttpConfiguration()
    val src = new SecureRequestCustomizer()
    src.setSniHostCheck(false)
    httpConfig.addCustomizer(src)
    val connectionFactories = AbstractConnectionFactory.getFactories(
      sslContextFactory, new HttpConnectionFactory(httpConfig))

    // Find the HttpConnectionFactory and verify its SecureRequestCustomizer
    val httpFactory = connectionFactories
      .find(_.isInstanceOf[HttpConnectionFactory])
      .map(_.asInstanceOf[HttpConnectionFactory])
      .getOrElse(fail("HttpConnectionFactory not found in SSL connection factories"))

    val customizers = httpFactory.getHttpConfiguration.getCustomizers
    val secureCustomizer = customizers.toArray
      .find(_.isInstanceOf[SecureRequestCustomizer])
      .map(_.asInstanceOf[SecureRequestCustomizer])
      .getOrElse(fail("SecureRequestCustomizer not found in HttpConfiguration"))

    assert(!secureCustomizer.isSniHostCheck,
      "SNI host check should be disabled for ThriftHttpCLIService SSL connections")
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
