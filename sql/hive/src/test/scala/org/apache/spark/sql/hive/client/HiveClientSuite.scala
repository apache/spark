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

package org.apache.spark.sql.hive.client

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.internal.SQLConf


class HiveClientSuite extends SparkFunSuite {

  test("metastore warehouse dir") {
    val emptyHiveConf = new HiveConf(new Configuration, classOf[Configuration])
    val emptySparkConf = new SparkConf
    val hiveConfWithValue: HiveConf = {
      val conf = new HiveConf(emptyHiveConf)
      conf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, "/hive")
      conf
    }
    val sparkConfWithValue: SparkConf = {
      val conf = new SparkConf
      conf.set(SQLConf.WAREHOUSE_PATH.key, "/spark")
      conf
    }
    val defaultValue: String = {
      val conf = new SQLConf
      emptySparkConf.getAll.foreach { case (k, v) => conf.setConfString(k, v) }
      conf.warehousePath
    }
    // Neither is set, should default to Spark's default value
    assert(HiveClientImpl.metastoreWarehousePath(emptyHiveConf, emptySparkConf) == defaultValue)
    // Only Hive conf is set, should use Hive value
    assert(HiveClientImpl.metastoreWarehousePath(hiveConfWithValue, emptySparkConf) == "/hive")
    // Only Spark conf is set, should use Spark value
    assert(HiveClientImpl.metastoreWarehousePath(emptyHiveConf, sparkConfWithValue) == "/spark")
    // Both are set, should use Spark value
    assert(HiveClientImpl.metastoreWarehousePath(hiveConfWithValue, sparkConfWithValue) == "/spark")
  }

}
