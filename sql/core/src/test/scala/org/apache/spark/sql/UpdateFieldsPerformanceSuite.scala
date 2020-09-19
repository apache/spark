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

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class UpdateFieldsPerformanceSuite extends QueryTest with SharedSparkSession {

  private def colName(d: Int, colNum: Int): String = s"nested${d}Col$colNum"

  private def nestedStructType(
    depths: Seq[Int], colNums: Seq[Int], nullable: Boolean): StructType = {
    if (depths.length == 1) {
      StructType(colNums.map { colNum =>
        StructField(colName(depths.head, colNum), IntegerType, nullable = false)
      })
    } else {
      val depth = depths.head
      val fields = colNums.foldLeft(Seq.empty[StructField]) {
        case (structFields, colNum) if colNum == 0 =>
          val nested = nestedStructType(depths.tail, colNums, nullable = nullable)
          structFields :+ StructField(colName(depth, colNum), nested, nullable = nullable)
        case (structFields, colNum) =>
          structFields :+ StructField(colName(depth, colNum), IntegerType, nullable = false)
      }
      StructType(fields)
    }
  }

  private def nestedRow(depths: Seq[Int], colNums: Seq[Int]): Row = {
    if (depths.length == 1) {
      Row.fromSeq(colNums)
    } else {
      val values = colNums.foldLeft(Seq.empty[Any]) {
        case (values, colNum) if colNum == 0 => values :+ nestedRow(depths.tail, colNums)
        case (values, colNum) => values :+ colNum
      }
      Row.fromSeq(values)
    }
  }

  /**
   * Utility function for generating a DataFrame with nested columns.
   *
   * @param depth: The depth to which to create nested columns.
   * @param numColsAtEachDepth: The number of columns to create at each depth. The columns names
   *                          are in the format of nested${depth}Col${index}. The value of each
   *                          column will be its index at that depth, or if the index of the column
   *                          is 0, then the value could also be a struct.
   * @param nullable: This value is used to set the nullability of StructType columns.
   */
  private def nestedDf(
    depth: Int, numColsAtEachDepth: Int, nullable: Boolean = false): DataFrame = {
    require(depth > 0)
    require(numColsAtEachDepth > 0)

    val depths = 1 to depth
    val colNums = 0 until numColsAtEachDepth
    val nestedColumn = nestedRow(depths, colNums)
    val nestedColumnDataType = nestedStructType(depths, colNums, nullable)

    spark.createDataFrame(
      sparkContext.parallelize(Row(nestedColumn) :: Nil),
      StructType(Seq(StructField(colName(0, 0), nestedColumnDataType, nullable = nullable))))
  }

  test("nestedDf should generate nested DataFrames") {
    checkAnswer(
      nestedDf(1, 1),
      Row(Row(0)) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      nestedDf(1, 2),
      Row(Row(0, 1)) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", IntegerType, nullable = false),
        StructField("nested1Col1", IntegerType, nullable = false))),
        nullable = false))))

    checkAnswer(
      nestedDf(2, 1),
      Row(Row(Row(0))) :: Nil,
      StructType(Seq(StructField("nested0Col0", StructType(Seq(
        StructField("nested1Col0", StructType(Seq(
          StructField("nested2Col0", IntegerType, nullable = false))),
          nullable = false))),
        nullable = false))))

    checkAnswer(
      nestedDf(2, 2),
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

  // simulates how a user would add/drop nested fields in a performant manner
  private def addDropNestedColumns(
    column: Column,
    depths: Seq[Int],
    colNumsToAdd: Seq[Int] = Seq.empty,
    colNumsToDrop: Seq[Int] = Seq.empty): Column = {
    val depth = depths.head

    // drop columns at the current depth
    val dropped = if (colNumsToDrop.nonEmpty) {
      column.dropFields(colNumsToDrop.map(num => colName(depth, num)): _*)
    } else column

    // add columns at the current depth
    val added = colNumsToAdd.foldLeft(dropped) {
      (res, num) => res.withField(colName(depth, num), lit(num))
    }

    if (depths.length == 1) {
      added
    } else {
      // add/drop columns at the next depth
      val nestedColumn = col((0 to depth).map(d => s"`${colName(d, 0)}`").mkString("."))
      added.withField(
        colName(depth, 0),
        addDropNestedColumns(nestedColumn, depths.tail, colNumsToAdd, colNumsToDrop))
    }
  }

  // check both nullable and non-nullable struct code paths are performant
  Seq(true, false).foreach { nullable =>
    test("should add 5 columns at 20 different depths of nesting for a total of 100 columns " +
      s"added, nullable = $nullable") {
      val maxDepth = 20

      // dataframe with nested*Col0 to nested*Col4 at each of 20 depths
      val inputDf = nestedDf(maxDepth, 5, nullable = nullable)

      // add nested*Col5 through nested*Col9 at each depth
      val resultDf = inputDf.select(addDropNestedColumns(
        column = col(colName(0, 0)),
        depths = 1 to maxDepth,
        colNumsToAdd = 5 to 9).as("nested0Col0"))

      // dataframe with nested*Col0 to nested*Col9 at each of 20 depths
      val expectedDf = nestedDf(maxDepth, 10, nullable = nullable)
      checkAnswer(resultDf, expectedDf.collect(), expectedDf.schema)
    }

    test("should drop 5 columns at 20 different depths of nesting for a total of 100 columns " +
      s"dropped, nullable = $nullable") {
      val maxDepth = 20

      // dataframe with nested*Col0 to nested*Col9 at each of 20 depths
      val inputDf = nestedDf(maxDepth, 10, nullable = nullable)

      // drop nested*Col5 to nested*Col9 at each of 20 depths
      val resultDf = inputDf.select(addDropNestedColumns(
        column = col(colName(0, 0)),
        depths = 1 to maxDepth,
        colNumsToDrop = 5 to 9).as("nested0Col0"))

      // dataframe with nested*Col0 to nested*Col4 at each of 20 depths
      val expectedDf = nestedDf(maxDepth, 5, nullable = nullable)
      checkAnswer(resultDf, expectedDf.collect(), expectedDf.schema)
    }

    test("should add 5 columns and drop 5 columns at 20 different depths of nesting for a total " +
      s"of 200 columns added/dropped, nullable = $nullable") {
      val maxDepth = 20

      // dataframe with nested*Col0 to nested*Col9 at each of 20 depths
      val inputDf = nestedDf(maxDepth, 10, nullable = nullable)

      // add nested*Col10 through nested*Col14 at each depth
      // drop nested*Col5 through nested*Col9 at each depth
      val resultDf = inputDf.select(addDropNestedColumns(
        column = col(colName(0, 0)),
        depths = 1 to maxDepth,
        colNumsToAdd = 10 to 14,
        colNumsToDrop = 5 to 9).as("nested0Col0"))

      // dataframe with nested*Col0 to nested*Col4 and nested*Col10 to nested*Col14
      // at each of 20 depths
      val expectedDf = {
        val depths = 1 to maxDepth
        val numCols = (0 to 4) ++ (10 to 14)
        val nestedColumn = nestedRow(depths, numCols)
        val nestedColumnDataType = nestedStructType(depths, numCols, nullable = nullable)

        spark.createDataFrame(
          sparkContext.parallelize(Row(nestedColumn) :: Nil),
          StructType(Seq(StructField(colName(0, 0), nestedColumnDataType, nullable = nullable))))
      }
      checkAnswer(resultDf, expectedDf.collect(), expectedDf.schema)
    }
  }
}
