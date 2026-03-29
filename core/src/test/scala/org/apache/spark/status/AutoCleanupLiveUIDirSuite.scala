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

package org.apache.spark.status

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.Status.LIVE_UI_LOCAL_STORE_DIR
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.util.Utils

class AutoCleanupLiveUIDirSuite extends SparkFunSuite {

  test("SPARK-41694: Auto cleanup Spark UI store path") {
    val baseUIDir = Utils.createTempDir()
    try {
      val conf = new SparkConf().setAppName("ui-dir-cleanup").setMaster("local")
        .set(LIVE_UI_LOCAL_STORE_DIR, baseUIDir.getCanonicalPath)
      val sc = new SparkContext(conf)
      sc.parallelize(0 until 100, 10)
        .map { x => (x % 10) -> x }
        .reduceByKey {
          _ + _
        }
        .collect()
      // `baseUIDir` should exists and not empty before SparkContext stop.
      assert(baseUIDir.exists())
      val subDirs = baseUIDir.listFiles()
      assert(subDirs.nonEmpty)
      val uiDirs = subDirs.filter(_.getName.startsWith("spark-ui"))
      assert(uiDirs.length == 1)
      assert(uiDirs.head.listFiles().nonEmpty)
      sc.stop()
      // base dir should exists
      assert(baseUIDir.exists())
      assert(!uiDirs.head.exists())
      assert(baseUIDir.listFiles().isEmpty)
    } finally {
      JavaUtils.deleteRecursively(baseUIDir)
      assert(!baseUIDir.exists())
    }
  }
}
