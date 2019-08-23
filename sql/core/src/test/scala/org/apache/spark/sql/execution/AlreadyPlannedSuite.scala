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

import org.apache.spark.sql.test.SharedSparkSession

class AlreadyPlannedSuite extends SparkPlanTest with SharedSparkSession {

  import testImplicits._

  test("simple execution") {
    val df = spark.range(10)
    val planned = AlreadyPlanned.dataFrame(spark, df.queryExecution.executedPlan)

    checkAnswer(planned, identity, df.toDF().collect())
  }

  test("planning on top - filter") {
    val df = spark.range(10)
    val planned = AlreadyPlanned.dataFrame(spark, df.queryExecution.executedPlan)

    checkAnswer(planned.where('id < 5), identity, df.toDF().where('id < 5).collect())
  }

  test("planning on top - joins") {
    val df = spark.range(10)
    val planned = AlreadyPlanned.dataFrame(spark, df.queryExecution.executedPlan)

    val join = planned.where('id < 3).alias("l").join(planned.alias("r"), Seq("id"))
    val expected = df.where('id < 3).alias("l").join(df.alias("r"), Seq("id"))

    checkAnswer(join, identity, expected.collect())

    val useJoinedPlan = AlreadyPlanned.dataFrame(spark, expected.queryExecution.executedPlan)
    checkAnswer(useJoinedPlan, identity, expected.collect())
    checkAnswer(useJoinedPlan.where('id < 2), identity, expected.where('id < 2).collect())
  }
}
