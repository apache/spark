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

import org.apache.hadoop.hive.conf.HiveConf

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.sql.SQLInsertTestSuite
import org.apache.spark.sql.hive.test.TestHiveSingleton

class HiveSQLInsertTestSuite extends SQLInsertTestSuite with TestHiveSingleton {

  private val originalPartitionMode = spark.conf.getOption("hive.exec.dynamic.partition.mode")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }

  override protected def afterAll(): Unit = {
    originalPartitionMode
      .map(v => spark.conf.set("hive.exec.dynamic.partition.mode", v))
      .getOrElse(spark.conf.unset("hive.exec.dynamic.partition.mode"))
    super.afterAll()
  }

  override def format: String = "hive OPTIONS(fileFormat='parquet')"

  override def checkV1AndV2Error(
      exception: SparkThrowable,
      v1ErrorClass: String,
      v2ErrorClass: String,
      v1Parameters: Map[String, String],
      v2Parameters: Map[String, String]): Unit = {
    checkError(exception = exception, sqlState = None, condition = v1ErrorClass,
      parameters = v1Parameters)
  }

  test("SPARK-54853: SET hive.exec.max.dynamic.partitions takes effect in session conf") {
    withSQLConf(
      HiveUtils.CONVERT_INSERTING_PARTITIONED_TABLE.key -> "false") {
      val cols = Seq("c1", "p1")
      val df = sql("SELECT 1, * FROM range(3)")
      Seq(true, false).foreach { overwrite =>
        withTable("t1") {
          createTable("t1", cols, Seq("int", "int"), cols.takeRight(1))
          assert(spark.table("t1").count() === 0)

          spark.conf.set("hive.exec.max.dynamic.partitions", "3")
          processInsert("t1", df, overwrite = overwrite)
          assert(spark.table("t1").count() === 3)

          spark.conf.set("hive.exec.max.dynamic.partitions", "2")
          checkError(
            exception = intercept[SparkException] {
              processInsert("t1", df, overwrite = overwrite)
            },
            condition = "DYNAMIC_PARTITION_WRITE_PARTITION_NUM_LIMIT_EXCEEDED",
            sqlState = Some("54054"),
            parameters = Map(
              "numWrittenParts" -> "3",
              "maxDynamicPartitionsKey" -> HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname,
              "maxDynamicPartitions" -> "2"))
          assert(spark.table("t1").count() === 3)

          spark.conf.set("hive.exec.max.dynamic.partitions", "3")
          processInsert("t1", df, overwrite = overwrite)
          val expectedRowCount = if (overwrite) 3 else 6
          assert(spark.table("t1").count() === expectedRowCount)
        }
      }
    }
  }
}
