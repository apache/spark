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

  def nestedColName(d: Int, colNum: Int): String = s"nested${d}Col$colNum"

  def nestedStructType(
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

  /**
   * Utility function for generating an empty DataFrame with nested columns.
   *
   * @param maxDepth: The depth to which to create nested columns.
   * @param numColsAtEachDepth: The number of columns to create at each depth.
   * @param nullable: This value is used to set the nullability of any StructType columns.
   */
  def emptyNestedDf(maxDepth: Int, numColsAtEachDepth: Int, nullable: Boolean): DataFrame = {
    require(maxDepth > 0)
    require(numColsAtEachDepth > 0)

    val nestedColumnDataType = nestedStructType(0 until numColsAtEachDepth, nullable, maxDepth)
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      StructType(Seq(StructField(nestedColName(0, 0), nestedColumnDataType, nullable))))
  }

  trait ModifyNestedColumns {
    val name: String
    def apply(column: Column, numsToAdd: Seq[Int], numsToDrop: Seq[Int], maxDepth: Int): Column
  }

  object Performant extends ModifyNestedColumns {
    override val name: String = "performant"

    override def apply(
        column: Column,
        numsToAdd: Seq[Int],
        numsToDrop: Seq[Int],
        maxDepth: Int): Column = helper(column, numsToAdd, numsToDrop, maxDepth, 1)

    private def helper(
        column: Column,
        numsToAdd: Seq[Int],
        numsToDrop: Seq[Int],
        maxDepth: Int,
        currDepth: Int): Column = {

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
        val newValue = helper(
          column = col((0 to currDepth).map(d => nestedColName(d, 0)).mkString(".")),
          numsToAdd = numsToAdd,
          numsToDrop = numsToDrop,
          currDepth = currDepth + 1,
          maxDepth = maxDepth)
        added.withField(nestedColName(currDepth, 0), newValue)
      }
    }
  }

  object NonPerformant extends ModifyNestedColumns {
    override val name: String = "non-performant"

    override def apply(
        column: Column,
        numsToAdd: Seq[Int],
        numsToDrop: Seq[Int],
        maxDepth: Int): Column = {

      val dropped = if (numsToDrop.nonEmpty) {
        val colsToDrop = (1 to maxDepth).flatMap { depth =>
          numsToDrop.map(num => s"${prefix(depth)}${nestedColName(depth, num)}")
        }
        column.dropFields(colsToDrop: _*)
      } else column

      val added = {
        val colsToAdd = (1 to maxDepth).flatMap { depth =>
          numsToAdd.map(num => (s"${prefix(depth)}${nestedColName(depth, num)}", lit(num)))
        }
        colsToAdd.foldLeft(dropped)((col, add) => col.withField(add._1, add._2))
      }

      added
    }

    private def prefix(depth: Int): String =
      if (depth == 1) ""
      else (1 until depth).map(d => nestedColName(d, 0)).mkString("", ".", ".")
  }

  private def updateFieldsBenchmark(
      methods: Seq[ModifyNestedColumns],
      maxDepth: Int,
      initialNumberOfColumns: Int,
      numsToAdd: Seq[Int] = Seq.empty,
      numsToDrop: Seq[Int] = Seq.empty): Unit = {

    val name = s"Add ${numsToAdd.length} columns and drop ${numsToDrop.length} columns " +
      s"at $maxDepth different depths of nesting"

    runBenchmark(name) {
      val benchmark = new Benchmark(
        name = name,
        // The purpose of this benchmark is to ensure Spark is able to analyze and optimize long
        // UpdateFields chains quickly so it runs over 0 rows of data.
        valuesPerIteration = 0,
        output = output)

      val nonNullableStructsDf = emptyNestedDf(maxDepth, initialNumberOfColumns, nullable = false)
      val nullableStructsDf = emptyNestedDf(maxDepth, initialNumberOfColumns, nullable = true)

      methods.foreach { method =>
        val modifiedColumn = method(
          column = col(nestedColName(0, 0)),
          numsToAdd = numsToAdd,
          numsToDrop = numsToDrop,
          maxDepth = maxDepth
        ).as(nestedColName(0, 0))

        benchmark.addCase(s"To non-nullable StructTypes using ${method.name} method") { _ =>
          nonNullableStructsDf.select(modifiedColumn).queryExecution.optimizedPlan
        }

        benchmark.addCase(s"To nullable StructTypes using ${method.name} method") { _ =>
          nullableStructsDf.select(modifiedColumn).queryExecution.optimizedPlan
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    // This benchmark compares the performant and non-performant methods of writing the same query.
    // We use small values for maxDepth, numsToAdd, and numsToDrop because the NonPerformant method
    // scales extremely poorly with the number of nested columns being added/dropped.
    updateFieldsBenchmark(
      methods = Seq(Performant, NonPerformant),
      maxDepth = 3,
      initialNumberOfColumns = 5,
      numsToAdd = 5 to 6,
      numsToDrop = 3 to 4)

    // This benchmark is to show that the performant method of writing a query when we want to add
    // and drop a large number of nested columns scales nicely.
    updateFieldsBenchmark(
      methods = Seq(Performant),
      maxDepth = 100,
      initialNumberOfColumns = 51,
      numsToAdd = 51 to 100,
      numsToDrop = 1 to 50)
  }
}
