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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `SHOW PARTITIONS` command to check V2 table catalogs.
 */
class ShowPartitionsSuite extends command.ShowPartitionsSuiteBase with CommandSuiteBase {
  test("a table does not support partitioning") {
    val table = s"non_part_$catalog.tab1"
    withTable(table) {
      sql(s"""
        |CREATE TABLE $table (price int, qty int, year int, month int)
        |$defaultUsing""".stripMargin)
      val errMsg = intercept[AnalysisException] {
        sql(s"SHOW PARTITIONS $table")
      }.getMessage
      assert(errMsg.contains(s"Table $table does not support partition management"))
    }
  }

  test("SPARK-33889, SPARK-33904: null and empty string as partition values") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createNullPartTable(t, "parquet")
      runShowPartitionsSql(s"SHOW PARTITIONS $t", Row("part=") :: Row("part=null") :: Nil)
      checkAnswer(spark.table(t), Row(0, "") :: Row(1, null) :: Nil)
    }
  }
}
