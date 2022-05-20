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

package org.apache.spark

import java.io.File
import java.util.UUID

import org.apache.spark.util.Utils


trait LocalRootDirsTest extends SparkFunSuite with LocalSparkContext {

  val conf = new SparkConf(loadDefaults = false)

  protected var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Once 'spark.local.dir' is set, it is cached. Unless this is manually cleared
    // before/after a test, it could return the same directory even if this property
    // is configured.
    Utils.clearLocalRootDirs()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Utils.createTempDir(namePrefix = "local")
    conf.set("spark.local.dir",
      tempDir.getAbsolutePath + File.separator + UUID.randomUUID().toString)
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(tempDir)
      Utils.clearLocalRootDirs()
    } finally {
      super.afterEach()
    }
  }
}
