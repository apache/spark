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

import scala.util.Random

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._


class TakeOrderedAndProjectSuite extends SparkPlanTest with SharedSQLContext {

  private var rand: Random = _
  private var seed: Long = 0

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    seed = System.currentTimeMillis()
    rand = new Random(seed)
  }

  private def generateRandomInputData(): DataFrame = {
    val schema = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", IntegerType, nullable = false)
    val inputData = Seq.fill(10000)(Row(rand.nextInt(), rand.nextInt()))
    spark.createDataFrame(sparkContext.parallelize(Random.shuffle(inputData), 10), schema)
  }

  /**
   * Adds a no-op filter to the child plan in order to prevent executeCollect() from being
   * called directly on the child plan.
   */
  private def noOpFilter(plan: SparkPlan): SparkPlan = FilterExec(Literal(true), plan)

  val limit = 250
  val sortOrder = 'a.desc :: 'b.desc :: Nil

  test("TakeOrderedAndProject.doExecute without project") {
    withClue(s"seed = $seed") {
      checkThatPlansAgree(
        generateRandomInputData(),
        input =>
          noOpFilter(TakeOrderedAndProjectExec(limit, sortOrder, None, input)),
        input =>
          GlobalLimitExec(limit,
            LocalLimitExec(limit,
              SortExec(sortOrder, true, input))),
        sortAnswers = false)
    }
  }

  test("TakeOrderedAndProject.doExecute with project") {
    withClue(s"seed = $seed") {
      checkThatPlansAgree(
        generateRandomInputData(),
        input =>
          noOpFilter(
            TakeOrderedAndProjectExec(limit, sortOrder, Some(Seq(input.output.last)), input)),
        input =>
          GlobalLimitExec(limit,
            LocalLimitExec(limit,
              ProjectExec(Seq(input.output.last),
                SortExec(sortOrder, true, input)))),
        sortAnswers = false)
    }
  }
}
