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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._


class ProjectSortOrderSuite extends SparkPlanTest with SharedSQLContext {

  private def generateInputData(): DataFrame = {
    val schema = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", IntegerType, nullable = false)
    val inputData = Seq(1, 2, 3).flatMap(i => Seq(Row(i, i + 1), Row(i, i + 2)))
    spark.createDataFrame(sparkContext.parallelize(inputData, 10), schema)
  }

  val limit = 2

  test("TakeOrderedAndProjectExec should have correct outputOrdering") {
    val df = generateInputData()
    val input = df.queryExecution.sparkPlan
    val sortOrder = Seq(SortOrder(input.output(0), Ascending),
      SortOrder(input.output(1) + 1, Ascending))

    // The project list changes outputOrdering.
    val take1 = TakeOrderedAndProjectExec(limit, sortOrder,
      Seq(Alias(UnaryMinus(input.output(0)), "c")()), input)
    assert(take1.outputOrdering == List())

    val take2 = TakeOrderedAndProjectExec(limit, sortOrder, Seq(input.output(0)), input)
    assert(take2.outputOrdering == sortOrder(0) :: Nil)

    val take3 = TakeOrderedAndProjectExec(limit, sortOrder, input.output, input)
    assert(take3.outputOrdering == sortOrder(0) :: sortOrder(1) :: Nil)

    val take4 = TakeOrderedAndProjectExec(limit, sortOrder, Seq(input.output(1)), input)
    assert(take4.outputOrdering == List())

    // Alias doesn't change outputOrdering.
    val take5 = TakeOrderedAndProjectExec(limit, sortOrder,
      Seq(Alias(input.output(0), "c")()), input)
    assert(take5.outputOrdering == sortOrder(0) :: Nil)
  }

  test("Project should have correct outputOrdering") {
    val df = generateInputData()
    val sorted = df.sort(df("a"), df("b"))
    val input = sorted.queryExecution.sparkPlan
    val sortOrder = input.outputOrdering

    // The project list changes outputOrdering.
    val project1 = ProjectExec(Seq(Alias(UnaryMinus(input.output(0)), "c")()), input)
    assert(project1.outputOrdering == List())

    val project2 = ProjectExec(Seq(input.output(0)), input)
    assert(project2.outputOrdering == sortOrder(0) :: Nil)

    val project3 = ProjectExec(input.output, input)
    assert(project3.outputOrdering == sortOrder(0) :: sortOrder(1) :: Nil)

    val project4 = ProjectExec(Seq(input.output(1)), input)
    assert(project4.outputOrdering == List())

    // Alias doesn't change outputOrdering.
    val project5 = ProjectExec(Seq(Alias(input.output(0), "c")()), input)
    assert(project5.outputOrdering == sortOrder(0) :: Nil)
  }
}
