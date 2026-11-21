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
package org.apache.spark.status.api.v1

import java.net.URI

import jakarta.ws.rs.{GET, Path, Produces}
import jakarta.ws.rs.core.MediaType

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.internal.config.UI.{UI_ENABLED, UI_PORT, UI_PROMETHEUS_ENABLED}

/**
 * Tests which JAX-RS resources the live application UI serves under `/api` and `/metrics`.
 *
 * The resources are registered explicitly rather than by package scanning, so these tests pin
 * the routes that are expected to exist, and the ones that only existed because each context used
 * to scan the whole `org.apache.spark.status.api.v1` package.
 */
class ApiRootResourceRegistrationSuite extends SparkFunSuite with LocalSparkContext {

  private def startUI(prometheusEnabled: Boolean = true): String = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(getClass.getSimpleName)
      .set(UI_ENABLED, true)
      .set(UI_PORT, 0)
      .set(UI_PROMETHEUS_ENABLED, prometheusEnabled)
    sc = new SparkContext(conf)
    sc.uiWebUrl.get
  }

  private def responseCode(baseUrl: String, path: String): Int =
    TestUtils.httpResponseCode(new URI(baseUrl + path).toURL)

  private def responseBody(baseUrl: String, path: String): String =
    TestUtils.httpResponseMessage(new URI(baseUrl + path).toURL)

  test("registrars listed in META-INF/services are discovered") {
    assert(ApiRootResource.registeredRootResources.contains(classOf[TestExtensionRootResource]))
  }

  test("core REST API resources are served under /api") {
    val baseUrl = startUI()
    val appId = sc.applicationId
    assert(responseCode(baseUrl, "/api/v1/version") === 200)
    assert(responseCode(baseUrl, "/api/v1/applications") === 200)
    assert(responseCode(baseUrl, s"/api/v1/applications/$appId/allexecutors") === 200)
    assert(responseCode(baseUrl, s"/api/v1/applications/$appId/environment") === 200)
    assert(responseBody(baseUrl, "/api/v1/version").contains("\"spark\""))
  }

  test("root resources contributed through ApiRootResourceRegistrar are served under /api") {
    val baseUrl = startUI()
    assert(responseBody(baseUrl, "/api/v1/test-extension") === "ok")
    // The contributed resource shares the /v1 path with ApiRootResource; Jersey merges them, so
    // the core endpoints must still be served.
    assert(responseCode(baseUrl, "/api/v1/version") === 200)
  }

  test("Prometheus executor metrics are served under /metrics") {
    val baseUrl = startUI()
    assert(responseCode(baseUrl, "/metrics/executors/prometheus") === 200)
    assert(responseBody(baseUrl, "/metrics/executors/prometheus").contains("spark_info{"))
  }

  test("each context serves only its own root resources") {
    val baseUrl = startUI()
    // Package scanning used to register every root resource in both contexts.
    assert(responseCode(baseUrl, "/api/executors/prometheus") === 404)
    assert(responseCode(baseUrl, "/metrics/v1/version") === 404)
    assert(responseCode(baseUrl, "/metrics/v1/test-extension") === 404)
  }

  test("/metrics is not attached when spark.ui.prometheus.enabled is false") {
    val baseUrl = startUI(prometheusEnabled = false)
    assert(responseCode(baseUrl, "/api/v1/version") === 200)
    // Without a /metrics context, the request falls through to the UI's root redirect, so
    // assert on the attached handlers rather than on the HTTP status.
    val contextPaths = sc.ui.get.getHandlers.map(_.getContextPath)
    assert(contextPaths.contains("/api"))
    assert(!contextPaths.contains("/metrics"))
  }
}

@Path("/v1")
private[v1] class TestExtensionRootResource {
  @GET
  @Path("test-extension")
  @Produces(Array(MediaType.TEXT_PLAIN))
  def get(): String = "ok"
}

/** Registered in META-INF/services of the core test resources. */
private[spark] class TestApiRootResourceRegistrar extends ApiRootResourceRegistrar {
  override def rootResources: Seq[Class[_]] = Seq(classOf[TestExtensionRootResource])
}
