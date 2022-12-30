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

  test(s"auto cleanup spark ui store path") {
    val storePath = Utils.createTempDir()
    try {
      val conf = new SparkConf().setAppName("ui-dir-cleanup").setMaster("local")
        .set(LIVE_UI_LOCAL_STORE_DIR, storePath.getCanonicalPath)
      val sc = new SparkContext(conf)
      sc.parallelize(0 until 100, 10)
        .map { x => (x % 10) -> x }
        .reduceByKey {
          _ + _
        }
        .collect()
      // `storePath` should exists and not emtpy before SparkContext stop.
      assert(storePath.exists())
      assert(storePath.listFiles().nonEmpty)
      sc.stop()
      assert(storePath.exists())
      assert(storePath.listFiles().isEmpty)
    } finally {
      JavaUtils.deleteRecursively(storePath)
      assert(!storePath.exists())
    }
  }
}
