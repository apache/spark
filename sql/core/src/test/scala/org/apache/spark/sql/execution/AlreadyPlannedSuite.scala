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

package org.apache.spark.sql.execution

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.SharedSparkSession

class AlreadyPlannedSuite extends SparkPlanTest with SharedSparkSession {

  import testImplicits._

  test("simple execution") {
    val df = spark.range(10)
    val planned = AlreadyPlanned.dataFrame(spark, df.queryExecution.sparkPlan)

    checkAnswer(planned, identity, df.toDF().collect())
  }

  test("planning on top works - projection") {
    val df = spark.range(10)
    val planned = AlreadyPlanned.dataFrame(spark, df.queryExecution.sparkPlan)

    checkAnswer(
      planned.withColumn("data", 'id + 1),
      identity,
      df.withColumn("data", 'id + 1).collect())
  }

  test("planning on top works - filter") {
    val df = spark.range(10)
    val planned = AlreadyPlanned.dataFrame(spark, df.queryExecution.sparkPlan)

    checkAnswer(planned.where('id < 5), identity, df.where('id < 5).toDF().collect())
  }

  test("planning on top works - joins") {
    val df = spark.range(10)
    val planned = AlreadyPlanned.dataFrame(spark, df.queryExecution.sparkPlan)

    val plannedLeft = planned.alias("l")
    val dfLeft = df.alias("l")
    val plannedRight = planned.alias("r")
    val dfRight = df.alias("r")

    checkAnswer(
      plannedLeft.where('id < 3).join(plannedRight, Seq("id")),
      identity,
      dfLeft.where('id < 3).join(dfRight, Seq("id")).collect())

    checkAnswer(
      plannedLeft.where('id < 3).join(plannedRight, plannedLeft("id") === plannedRight("id")),
      identity,
      dfLeft.where('id < 3).join(dfRight, dfLeft("id") === dfRight("id")).collect())

    checkAnswer(
      plannedLeft.join(plannedRight, Seq("id")).where('id < 3),
      identity,
      dfLeft.join(dfRight, Seq("id")).where('id < 3).collect())

    checkAnswer(
      plannedLeft.join(plannedRight, plannedLeft("id") === plannedRight("id")).where($"l.id" < 3),
      identity,
      dfLeft.join(dfRight, dfLeft("id") === dfRight("id")).where($"l.id" < 3).collect())
  }
}
