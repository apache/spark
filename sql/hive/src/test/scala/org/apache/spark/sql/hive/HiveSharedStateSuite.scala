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

package org.apache.spark.sql.hive

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession

class HiveSharedStateSuite extends SparkFunSuite {

  test("the catalog should be determined at the very first") {
    SparkContext.getActive.foreach(_.stop())
    var sc: SparkContext = null
    try {
      val conf = new SparkConf().setMaster("local").setAppName("SharedState Test")
      sc = new SparkContext(conf)
      val ss = SparkSession.builder().enableHiveSupport().getOrCreate()
      assert(ss.sharedState.externalCatalog.unwrapped.getClass.getName ===
        "org.apache.spark.sql.hive.HiveExternalCatalog", "The catalog should be hive ")

      val ss2 = SparkSession.builder().getOrCreate()
      assert(ss2.sharedState.externalCatalog.unwrapped.getClass.getName ===
        "org.apache.spark.sql.hive.HiveExternalCatalog",
        "The catalog should be shared across sessions")
    } finally {
      if (sc != null && !sc.isStopped) {
        sc.stop()
      }
    }
  }
}
