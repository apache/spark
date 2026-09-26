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

package org.apache.spark.deploy

import java.io.File
import java.util.UUID

import org.apache.spark.{SecurityManager, SparkConf, SparkContext, SparkFunSuite, TestUtils}
import org.apache.spark.internal.config.{
  SHUFFLE_SERVICE_DB_ENABLED,
  SHUFFLE_SERVICE_ENABLED,
  SHUFFLE_SERVICE_PORT,
  SHUFFLE_SERVICE_REQUIRE_APP_SCOPED_LOCAL_DIRS}
import org.apache.spark.network.server.TransportServer
import org.apache.spark.util.Utils

class ExternalShuffleServiceLocalDirsSuite extends SparkFunSuite {

  test("validateLocalDirs only accepts localDirs under the configured local directories") {
    val sparkConf = new SparkConf()
      .set(SHUFFLE_SERVICE_ENABLED, true)
      .set(SHUFFLE_SERVICE_DB_ENABLED, false)
      .set("spark.local.dir", System.getProperty("java.io.tmpdir"))
    val service = new ExternalShuffleService(sparkConf, new SecurityManager(sparkConf))
    val handler = service.getBlockHandler

    // A localDir under one of the configured local directories is accepted.
    val root = new File(Utils.getConfiguredLocalDirs(sparkConf).head)
    val contained = new File(root, s"blockmgr-${UUID.randomUUID()}")
    assert(contained.mkdirs())
    try {
      handler.validateLocalDirs(Array(contained.getAbsolutePath), "app-contained")

      // A well-formed absolute localDir outside every configured root is rejected.
      intercept[IllegalArgumentException] {
        handler.validateLocalDirs(Array("/etc"), "app-outside")
      }
    } finally {
      Utils.deleteRecursively(contained)
    }
  }

  test("requireAppScopedLocalDirs restricts localDirs to the registering app's directory") {
    val sparkConf = new SparkConf()
      .set(SHUFFLE_SERVICE_ENABLED, true)
      .set(SHUFFLE_SERVICE_DB_ENABLED, false)
      .set(SHUFFLE_SERVICE_REQUIRE_APP_SCOPED_LOCAL_DIRS, true)
      .set("spark.local.dir", System.getProperty("java.io.tmpdir"))
    val service = new ExternalShuffleService(sparkConf, new SecurityManager(sparkConf))
    val handler = service.getBlockHandler

    val root = new File(Utils.getConfiguredLocalDirs(sparkConf).head)
    val appId = s"app-${UUID.randomUUID()}"
    val otherAppId = s"app-${UUID.randomUUID()}"
    // The layout the Worker creates: <root>/spark-<workerId>/<appId>.
    val workerRoot = new File(root, s"spark-worker-${UUID.randomUUID()}")
    val appScoped = new File(workerRoot, appId)
    val otherAppScoped = new File(workerRoot, otherAppId)
    // The pre-upgrade Worker layout: directly under a root, no appId segment.
    val unscoped = new File(root, s"blockmgr-${UUID.randomUUID()}")
    assert(appScoped.mkdirs() && otherAppScoped.mkdirs() && unscoped.mkdirs())
    try {
      // A localDir inside the registering application's own directory is accepted.
      handler.validateLocalDirs(Array(appScoped.getAbsolutePath), appId)

      // A directory under a different application id is rejected even though it is contained
      // under a configured root.
      intercept[IllegalArgumentException] {
        handler.validateLocalDirs(Array(otherAppScoped.getAbsolutePath), appId)
      }

      // A contained directory without the appId as a path segment (what a Worker without
      // the per-app layout would report) is rejected.
      intercept[IllegalArgumentException] {
        handler.validateLocalDirs(Array(unscoped.getAbsolutePath), appId)
      }

      // Root containment is still enforced with app scoping on.
      intercept[IllegalArgumentException] {
        handler.validateLocalDirs(Array("/etc"), appId)
      }
    } finally {
      Utils.deleteRecursively(workerRoot)
      Utils.deleteRecursively(unscoped)
    }
  }

  test("local-cluster executors register app-scoped local dirs end to end") {
    val root = Utils.createTempDir(namePrefix = "ess-e2e-root")
    val serviceConf = new SparkConf()
      .set(SHUFFLE_SERVICE_ENABLED, true)
      .set(SHUFFLE_SERVICE_DB_ENABLED, false)
      .set(SHUFFLE_SERVICE_REQUIRE_APP_SCOPED_LOCAL_DIRS, true)
      .set(SHUFFLE_SERVICE_PORT, 0)
      .set("spark.local.dir", root.getAbsolutePath)
    val service = new ExternalShuffleService(serviceConf, new SecurityManager(serviceConf))
    service.start()
    // ExternalShuffleService does not expose the bound port; read it back the same way
    // ExternalShuffleServiceMetricsSuite reads the metrics source.
    val serverField = classOf[ExternalShuffleService].getDeclaredField("server")
    serverField.setAccessible(true)
    val port = serverField.get(service).asInstanceOf[TransportServer].getPort

    val conf = new SparkConf()
      .setAppName("app-scoped-local-dirs-e2e")
      .setMaster("local-cluster[2,1,1024]")
      .set(SHUFFLE_SERVICE_ENABLED, true)
      .set(SHUFFLE_SERVICE_PORT, port)
      .set("spark.local.dir", root.getAbsolutePath)
    var sc: SparkContext = null
    try {
      sc = new SparkContext(conf)
      val appId = sc.applicationId
      // One core per worker, so the application's two executors land on different Workers
      // and each Worker creates its own <root>/spark-<workerId>/<appId>. If the shuffle
      // service rejected the registrations, the executors would never come up.
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
      assert(sc.parallelize(1 to 100, 4).map(i => (i % 10, i)).groupByKey().count() === 10)

      val appDirs = root.listFiles().toSeq
        .filter(d => d.isDirectory && d.getName.startsWith("spark-"))
        .flatMap(_.listFiles())
        .filter(d => d.isDirectory && d.getName == appId)
      // Each Worker scoped the application's local dir under its own per-worker directory.
      assert(appDirs.length == 2)
      assert(appDirs.map(_.getParentFile.getName).distinct.length == 2)
      // The registered dirs satisfy the app-scoping check for this application only.
      appDirs.foreach { d =>
        service.getBlockHandler.validateLocalDirs(Array(d.getAbsolutePath), appId)
        intercept[IllegalArgumentException] {
          service.getBlockHandler.validateLocalDirs(Array(d.getAbsolutePath), "other-app")
        }
      }
    } finally {
      if (sc != null) {
        sc.stop()
      }
      service.stop()
    }
  }
}
