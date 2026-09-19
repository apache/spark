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

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.internal.config.{SHUFFLE_SERVICE_DB_ENABLED, SHUFFLE_SERVICE_ENABLED}
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
}
