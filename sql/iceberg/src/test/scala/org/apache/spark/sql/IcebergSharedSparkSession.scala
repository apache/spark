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

package org.apache.spark.sql

import java.io.File

import org.scalatest.Suite

import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSessionBase
import org.apache.spark.util.Utils


trait IcebergSharedSparkSession extends SharedSparkSessionBase{
  self: Suite =>

  override protected def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.extensions",
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions").
      set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog").
      set("spark.sql.catalog.spark_catalog.type", "hadoop").
      set(SQLConf.PREFER_BROADCAST_VAR_PUSHDOWN_OVER_DPP.key, "false").
      set(SQLConf.PUSH_BROADCASTED_JOIN_KEYS_AS_FILTER_TO_SCAN.key, "true").
      set("spark.sql.catalog.spark_catalog.warehouse", makeWarehouseDir().toURI.getPath)


  def makeWarehouseDir(): File = {
    val warehouseDir = Utils.createTempDir(namePrefix = "iceberg_warehouse")
    warehouseDir.delete()
    warehouseDir
  }
}
