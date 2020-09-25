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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Benchmark to measure Spark's performance analyzing and optimizing long UpdateFields chains.
 *
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> <spark sql test jar>
 *   2. with sbt:
 *      build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *   Results will be written to "benchmarks/UpdateFieldsBenchmark-results.txt".
 * }}}
 */
object UpdateFieldsBenchmark extends SqlBasedBenchmark {

  private def nestedColName(d: Int, colNum: Int): String = s"nested${d}Col$colNum"

  private def nestedStructType(
      colNums: Seq[Int],
      nullable: Boolean,
      maxDepth: Int,
      currDepth: Int = 1): StructType = {

    if (currDepth == maxDepth) {
      val fields = colNums.map { colNum =>
        val name = nestedColName(currDepth, colNum)
        StructField(name, IntegerType, nullable = false)
      }
      StructType(fields)
    } else {
      val fields = colNums.foldLeft(Seq.empty[StructField]) {
        case (structFields, colNum) if colNum == 0 =>
          val nested = nestedStructType(colNums, nullable, maxDepth, currDepth + 1)
          structFields :+ StructField(nestedColName(currDepth, colNum), nested, nullable)
        case (structFields, colNum) =>
          val name = nestedColName(currDepth, colNum)
          structFields :+ StructField(name, IntegerType, nullable = false)
      }
      StructType(fields)
    }
  }

  private def nestedRow(colNums: Seq[Int], maxDepth: Int, currDepth: Int = 1): Row = {
    if (currDepth == maxDepth) {
      Row.fromSeq(colNums)
    } else {
      val values = colNums.foldLeft(Seq.empty[Any]) {
        case (values, colNum) if colNum == 0 =>
          values :+ nestedRow(colNums, maxDepth, currDepth + 1)
        case (values, colNum) =>
          values :+ colNum
      }
      Row.fromSeq(values)
    }
  }

  /**
   * Utility function for generating a DataFrame with nested columns.
   *
   * @param maxDepth: The depth to which to create nested columns.
   * @param numColsAtEachDepth: The number of columns to create at each depth. The value of each
   *                          column will be the same as its index (IntegerType) at that depth
   *                          unless the index = 0, in which case it may be a StructType which
   *                          represents the next depth.
   * @param nullable: This value is used to set the nullability of StructType columns.
   */
  def nestedDf(maxDepth: Int, numColsAtEachDepth: Int, nullable: Boolean): DataFrame = {
    require(maxDepth > 0)
    require(numColsAtEachDepth > 0)

    val colNums = 0 until numColsAtEachDepth
    val nestedColumn = nestedRow(colNums, maxDepth)
    val nestedColumnDataType = nestedStructType(colNums, nullable, maxDepth)

    spark.createDataFrame(
      spark.sparkContext.parallelize(Row(nestedColumn) :: Nil),
      StructType(Seq(StructField(nestedColName(0, 0), nestedColumnDataType, nullable))))
  }

  // simulates how a user would add/drop nested fields in a performant manner
  def modifyNestedColumns(
      column: Column,
      numsToAdd: Seq[Int],
      numsToDrop: Seq[Int],
      maxDepth: Int,
      currDepth: Int = 1): Column = {

    // drop columns at the current depth
    val dropped = if (numsToDrop.nonEmpty) {
      column.dropFields(numsToDrop.map(num => nestedColName(currDepth, num)): _*)
    } else column

    // add columns at the current depth
    val added = numsToAdd.foldLeft(dropped) {
      (res, num) => res.withField(nestedColName(currDepth, num), lit(num))
    }

    if (currDepth == maxDepth) {
      added
    } else {
      // add/drop columns at the next depth
      val newValue = modifyNestedColumns(
        column = col((0 to currDepth).map(d => nestedColName(d, 0)).mkString(".")),
        numsToAdd = numsToAdd,
        numsToDrop = numsToDrop,
        currDepth = currDepth + 1,
        maxDepth = maxDepth)
      added.withField(nestedColName(currDepth, 0), newValue)
    }
  }

  def updateFieldsBenchmark(
      maxDepth: Int,
      initialNumberOfColumns: Int,
      numsToAdd: Seq[Int] = Seq.empty,
      numsToDrop: Seq[Int] = Seq.empty): Unit = {

    val name = s"Add ${numsToAdd.length} columns and drop ${numsToDrop.length} columns " +
      s"at $maxDepth different depths of nesting"

    runBenchmark(name) {
      val benchmark = new Benchmark(
        name = name,
        // Because the point of this benchmark is only to ensure Spark is able to analyze and
        // optimize long UpdateFields chains quickly, this benchmark operates only over 1 row of
        // data.
        valuesPerIteration = 1,
        output = output)

      val modifiedColumn = modifyNestedColumns(
        col(nestedColName(0, 0)),
        numsToAdd,
        numsToDrop,
        maxDepth
      ).as(nestedColName(0, 0))

      val nonNullableInputDf = nestedDf(maxDepth, initialNumberOfColumns, nullable = false)
      val nullableInputDf = nestedDf(maxDepth, initialNumberOfColumns, nullable = true)

      benchmark.addCase("Non-Nullable StructTypes") { _ =>
        nonNullableInputDf.select(modifiedColumn).queryExecution.optimizedPlan
      }

      benchmark.addCase("Nullable StructTypes") { _ =>
        nullableInputDf.select(modifiedColumn).queryExecution.optimizedPlan
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val maxDepth = 20

    updateFieldsBenchmark(
      maxDepth = maxDepth,
      initialNumberOfColumns = 5,
      numsToAdd = 5 to 9)

    updateFieldsBenchmark(
      maxDepth = maxDepth,
      initialNumberOfColumns = 10,
      numsToDrop = 5 to 9)

    updateFieldsBenchmark(
      maxDepth = maxDepth,
      initialNumberOfColumns = 10,
      numsToAdd = 10 to 14,
      numsToDrop = 5 to 9)
  }
}

class UpdateFieldsBenchmark extends QueryTest with SharedSparkSession {
  import UpdateFieldsBenchmark._

  test("nestedDf should generate nested DataFrames") {
    checkAnswer(
      nestedDf(1, 1, nullable = false),
      Row(Row(0)) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      nestedDf(1, 2, nullable = false),
      Row(Row(0, 1)) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", IntegerType, nullable = false),
        StructField("nested1Col1", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      nestedDf(2, 1, nullable = false),
      Row(Row(Row(0))) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", StructType(Seq(
          StructField("nested2Col0", IntegerType, nullable = false))),
          nullable = false))),
        nullable = false))))

    checkAnswer(
      nestedDf(2, 2, nullable = false),
      Row(Row(Row(0, 1), 1)) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", StructType(Seq(
          StructField("nested2Col0", IntegerType, nullable = false),
          StructField("nested2Col1", IntegerType, nullable = false))),
          nullable = false),
        StructField("nested1Col1", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      nestedDf(2, 2, nullable = true),
      Row(Row(Row(0, 1), 1)) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", StructType(Seq(
          StructField("nested2Col0", IntegerType, nullable = false),
          StructField("nested2Col1", IntegerType, nullable = false))),
          nullable = true),
        StructField("nested1Col1", IntegerType, nullable = false))),
        nullable = true))))
  }

  private val maxDepth = 3

  test("modifyNestedColumns should add 5 columns at each depth of nesting") {
    // dataframe with nested*Col0 to nested*Col4 at each depth
    val inputDf = nestedDf(maxDepth, 5, nullable = false)

    // add nested*Col5 through nested*Col9 at each depth
    val resultDf = inputDf.select(modifyNestedColumns(
      column = col(nestedColName(0, 0)),
      numsToAdd = 5 to 9,
      numsToDrop = Seq.empty,
      maxDepth = maxDepth
    ).as("nested0Col0"))

    // dataframe with nested*Col0 to nested*Col9 at each depth
    val expectedDf = nestedDf(maxDepth, 10, nullable = false)
    checkAnswer(resultDf, expectedDf.collect(), expectedDf.schema)
  }

  test("modifyNestedColumns should drop 5 columns at each depth of nesting") {
    // dataframe with nested*Col0 to nested*Col9 at each depth
    val inputDf = nestedDf(maxDepth, 10, nullable = false)

    // drop nested*Col5 to nested*Col9 at each of 20 depths
    val resultDf = inputDf.select(modifyNestedColumns(
      column = col(nestedColName(0, 0)),
      numsToAdd = Seq.empty,
      numsToDrop = 5 to 9,
      maxDepth = maxDepth
    ).as("nested0Col0"))

    // dataframe with nested*Col0 to nested*Col4 at each depth
    val expectedDf = nestedDf(maxDepth, 5, nullable = false)
    checkAnswer(resultDf, expectedDf.collect(), expectedDf.schema)
  }

  test("modifyNestedColumns should add and drop 5 columns at each depth of nesting") {
    // dataframe with nested*Col0 to nested*Col9 at each depth
    val inputDf = nestedDf(maxDepth, 10, nullable = false)

    // drop nested*Col5 to nested*Col9 at each depth
    val resultDf = inputDf.select(modifyNestedColumns(
      column = col(nestedColName(0, 0)),
      numsToAdd = 10 to 14,
      numsToDrop = 5 to 9,
      maxDepth = maxDepth
    ).as("nested0Col0"))

    // dataframe with nested*Col0 to nested*Col4 and nested*Col10 to nested*Col14 at each depth
    val expectedDf = {
      val numCols = (0 to 4) ++ (10 to 14)
      val nestedColumn = nestedRow(numCols, maxDepth)
      val nestedColumnDataType = nestedStructType(numCols, nullable = false, maxDepth)

      spark.createDataFrame(
        sparkContext.parallelize(Row(nestedColumn) :: Nil),
        StructType(Seq(StructField(nestedColName(0, 0), nestedColumnDataType, nullable = false))))
    }
    checkAnswer(resultDf, expectedDf.collect(), expectedDf.schema)
  }
}
