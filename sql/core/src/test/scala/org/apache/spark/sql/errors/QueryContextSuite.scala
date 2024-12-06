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
package org.apache.spark.sql.errors

import org.apache.spark.{SparkArithmeticException, SparkConf}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class QueryContextSuite extends QueryTest with SharedSparkSession {
  override def sparkConf: SparkConf = super.sparkConf.set(SQLConf.ANSI_ENABLED.key, "true")

  private val ansiConf = "\"" + SQLConf.ANSI_ENABLED.key + "\""

  test("summary of DataFrame context") {
    withSQLConf(SQLConf.STACK_TRACES_IN_DATAFRAME_CONTEXT.key -> "2") {
      val e = intercept[SparkArithmeticException] {
        spark.range(1).select(lit(1) / lit(0)).collect()
      }
      assert(e.getQueryContext.head.summary() ==
        """== DataFrame ==
          |"div" was called from
          |org.apache.spark.sql.errors.QueryContextSuite.$anonfun$new$3(QueryContextSuite.scala:33)
          |org.scalatest.Assertions.intercept(Assertions.scala:749)
          |""".stripMargin)
    }
  }

  test("SPARK-50290: Add a flag to disable DataFrame context") {
    withSQLConf(SQLConf.DATA_FRAME_QUERY_CONTEXT_ENABLED.key -> "false") {
      val df = spark.range(1).select(lit(1) / col("id"))
      checkError(
        exception = intercept[SparkArithmeticException](df.collect()),
        condition = "DIVIDE_BY_ZERO",
        parameters = Map("config" -> ansiConf),
        context = ExpectedContext("", -1, -1)
      )
    }
  }
}
