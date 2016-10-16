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

import org.apache.spark.sql.{Column, DataFrame, Row}
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

    // Sort order: ["a".asc, "b + 1".asc]
    val sortOrder = Seq(SortOrder(input.output(0), Ascending),
      SortOrder(input.output(1) + 1, Ascending))

    // The project list changes outputOrdering.
    // Project list: [-"a" + 1]
    // Sort order: Nil
    val take1 = TakeOrderedAndProjectExec(limit, sortOrder,
      Seq(Alias(UnaryMinus(input.output(0)), "c")()), input)
    assert(take1.outputOrdering == List())

    // Project list: ["a"]
    // Sort order: ["a".asc]
    val take2 = TakeOrderedAndProjectExec(limit, sortOrder, Seq(input.output(0)), input)
    assert(take2.outputOrdering == sortOrder(0) :: Nil)

    // Project list: ["a", "b"]
    // Sort order: ["a".asc, "b + 1".asc]
    val take3 = TakeOrderedAndProjectExec(limit, sortOrder, input.output, input)
    assert(take3.outputOrdering == sortOrder(0) :: sortOrder(1) :: Nil)

    // Project list: ["b"]
    // Sort order: Nil
    val take4 = TakeOrderedAndProjectExec(limit, sortOrder, Seq(input.output(1)), input)
    assert(take4.outputOrdering == List())

    // Alias doesn't change outputOrdering.
    // Project list: ["a" as "c"]
    // Sort order: ["a".asc]
    val take5 = TakeOrderedAndProjectExec(limit, sortOrder,
      Seq(Alias(input.output(0), "c")()), input)
    assert(take5.outputOrdering == sortOrder(0) :: Nil)
  }

  test("Project should have correct outputOrdering") {
    val df = generateInputData()
    val sorted = df.sort(Column(df("b").expr + 1).desc, df("a").asc)
    val input = sorted.queryExecution.sparkPlan
    // Sort order: ["b + 1".desc, "a".asc]
    val sortOrder = input.outputOrdering

    // The project list changes outputOrdering.
    // Project list: [-"a" as "c"]
    // Sort order: Nil
    val project1 = ProjectExec(Seq(Alias(UnaryMinus(input.output(0)), "c")()), input)
    assert(project1.outputOrdering == List())

    // Project list: [a]
    // Sort order: Nil
    val project2 = ProjectExec(Seq(input.output(0)), input)
    assert(project2.outputOrdering == List())

    // Project list: [b]
    // Sort order: "b + 1".desc
    val project3 = ProjectExec(Seq(input.output(1)), input)
    assert(project3.outputOrdering == sortOrder(0) :: Nil)

    // Project list: [a, b]
    // Sort order: ["b + 1".desc, "a".asc]
    val project4 = ProjectExec(input.output, input)
    assert(project4.outputOrdering == sortOrder(0) :: sortOrder(1) :: Nil)

    // Project list: [b, a]
    // Sort order: ["b + 1".desc, "a".asc]
    val project5 = ProjectExec(input.output.reverse, input)
    assert(project5.outputOrdering == sortOrder(0) :: sortOrder(1) :: Nil)

    // Alias doesn't change outputOrdering.
    // Project list: ["b" as "c"]
    // Sort order: "b + 1".desc
    val project6 = ProjectExec(Seq(Alias(input.output(1), "c")()), input)
    assert(project6.outputOrdering == sortOrder(0) :: Nil)
  }
}
