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

import org.apache.spark.AccumulatorSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

/**
 * Test sorting. Many of the test cases generate random data and compares the sorted result with one
 * sorted by a reference implementation ([[ReferenceSort]]).
 */
class SortSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder

  test("basic sorting using ExternalSort") {

    val input = Seq(
      ("Hello", 4, 2.0),
      ("Hello", 1, 1.0),
      ("World", 8, 3.0)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => SortExec('a.asc :: 'b.asc :: Nil, global = true, child = child),
      input.sortBy(t => (t._1, t._2)).map(Row.fromTuple),
      sortAnswers = false)

    checkAnswer(
      input.toDF("a", "b", "c"),
      (child: SparkPlan) => SortExec('b.asc :: 'a.asc :: Nil, global = true, child = child),
      input.sortBy(t => (t._2, t._1)).map(Row.fromTuple),
      sortAnswers = false)
  }

  test("sorting all nulls") {
    checkThatPlansAgree(
      (1 to 100).map(v => Tuple1(v)).toDF().selectExpr("NULL as a"),
      (child: SparkPlan) =>
        GlobalLimitExec(10, SortExec('a.asc :: Nil, global = true, child = child)),
      (child: SparkPlan) =>
        GlobalLimitExec(10, ReferenceSort('a.asc :: Nil, global = true, child)),
      sortAnswers = false
    )
  }

  test("sort followed by limit") {
    checkThatPlansAgree(
      (1 to 100).map(v => Tuple1(v)).toDF("a"),
      (child: SparkPlan) =>
        GlobalLimitExec(10, SortExec('a.asc :: Nil, global = true, child = child)),
      (child: SparkPlan) =>
        GlobalLimitExec(10, ReferenceSort('a.asc :: Nil, global = true, child)),
      sortAnswers = false
    )
  }

  test("sorting does not crash for large inputs") {
    val sortOrder = 'a.asc :: Nil
    val stringLength = 1024 * 1024 * 2
    checkThatPlansAgree(
      Seq(Tuple1("a" * stringLength), Tuple1("b" * stringLength)).toDF("a").repartition(1),
      SortExec(sortOrder, global = true, _: SparkPlan, testSpillFrequency = 1),
      ReferenceSort(sortOrder, global = true, _: SparkPlan),
      sortAnswers = false
    )
  }

  test("sorting updates peak execution memory") {
    AccumulatorSuite.verifyPeakExecutionMemorySet(sparkContext, "unsafe external sort") {
      checkThatPlansAgree(
        (1 to 100).map(v => Tuple1(v)).toDF("a"),
        (child: SparkPlan) => SortExec('a.asc :: Nil, global = true, child = child),
        (child: SparkPlan) => ReferenceSort('a.asc :: Nil, global = true, child),
        sortAnswers = false)
    }
  }

  // Test sorting on different data types
  for (
    dataType <- DataTypeTestUtils.atomicTypes ++ Set(NullType);
    nullable <- Seq(true, false);
    sortOrder <-
      Seq('a.asc :: Nil, 'a.asc_nullLast :: Nil, 'a.desc :: Nil, 'a.desc_nullFirst :: Nil);
    randomDataGenerator <- RandomDataGenerator.forType(dataType, nullable)
  ) {
    test(s"sorting on $dataType with nullable=$nullable, sortOrder=$sortOrder") {
      val inputData = Seq.fill(1000)(randomDataGenerator())
      val inputDf = spark.createDataFrame(
        sparkContext.parallelize(Random.shuffle(inputData).map(v => Row(v))),
        StructType(StructField("a", dataType, nullable = true) :: Nil)
      )
      checkThatPlansAgree(
        inputDf,
        p => SortExec(sortOrder, global = true, p: SparkPlan, testSpillFrequency = 23),
        ReferenceSort(sortOrder, global = true, _: SparkPlan),
        sortAnswers = false
      )
    }
  }
}
