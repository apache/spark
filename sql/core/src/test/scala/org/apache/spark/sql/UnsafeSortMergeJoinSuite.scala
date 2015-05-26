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

import org.apache.spark.sql.TestData._
import org.apache.spark.sql.execution.UnsafeExternalSort
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.scalatest.BeforeAndAfterEach

class UnsafeSortMergeJoinSuite extends QueryTest with BeforeAndAfterEach {
  // Ensures tables are loaded.
  TestData

  conf.setConf(SQLConf.SORTMERGE_JOIN, "true")
  conf.setConf(SQLConf.CODEGEN_ENABLED, "true")
  conf.setConf(SQLConf.UNSAFE_ENABLED, "true")
  conf.setConf(SQLConf.EXTERNAL_SORT, "true")
  conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, "-1")

  test("basic sort merge join test") {
    val df = upperCaseData.join(lowerCaseData, $"n" === $"N")
    print(df.queryExecution.optimizedPlan)
    assert(df.queryExecution.sparkPlan.collect {
      case smj: UnsafeSortMergeJoin => smj
    }.nonEmpty)
    checkAnswer(
      df,
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")
      ))
  }
}
