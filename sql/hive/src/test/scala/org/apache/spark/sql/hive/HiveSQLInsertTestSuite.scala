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

import org.apache.spark.sql.{AnalysisException, SQLInsertTestSuite}
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

  test("insert with column list - missing columns for HiveSQL") {
    val cols = Seq("c1", "c2", "c3", "c4")

    withTable("t1") {
      createTable("t1", cols, Seq.fill(4)("int"))
      checkError(
        exception = intercept[AnalysisException](sql(s"INSERT INTO t1 (c1) values(1)")),
        errorClass = "_LEGACY_ERROR_TEMP_1168",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t1`",
          "targetColumns" -> "4",
          "insertedColumns" -> "1",
          "staticPartCols" -> "0")
      )
    }

    withTable("t1") {
      createTable("t1", cols, Seq.fill(4)("int"), cols.takeRight(2))
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO t1 partition(c3=3, c4=4) (c1) values(1)")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1168",
        parameters = Map(
          "tableName" -> "`spark_catalog`.`default`.`t1`",
          "targetColumns" -> "4",
          "insertedColumns" -> "3",
          "staticPartCols" -> "2")
      )
    }
  }
}
