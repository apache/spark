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

package org.apache.spark.sql.execution.datasources

import java.io.File

import org.apache.spark.sql.{DataFrame, Row}


/**
 * Add a nested column.
 */
trait AddNestedColumnTest extends ReadSchemaTest {

  private def testAdd(df1: DataFrame, df2: DataFrame, expectedRows: Seq[Row]): Unit = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val dir1 = s"$path${File.separator}part=one"
      val dir2 = s"$path${File.separator}part=two"

      df1.write.format(format).options(options).save(dir1)
      df2.write.format(format).options(options).save(dir2)

      val df = spark.read
        .schema(df2.schema)
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, expectedRows)
    }
  }

  test("add a nested column at the end of the leaf struct column") {
    testAdd(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4, 'c7', 5)) c2"),
      Seq(
        Row(1, Row(2, Row(3, 4, null)), "one"),
        Row(1, Row(2, Row(3, 4, 5)), "two")))
  }

  test("add a nested column in the middle of the leaf struct column") {
    testAdd(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c7', 5, 'c6', 4)) c2"),
      Seq(
        Row(1, Row(2, Row(3, null, 4)), "one"),
        Row(1, Row(2, Row(3, 5, 4)), "two")))
  }

  test("add a nested column at the end of the middle struct column") {
    testAdd(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4), 'c7', 5) c2"),
      Seq(
        Row(1, Row(2, Row(3, 4), null), "one"),
        Row(1, Row(2, Row(3, 4), 5), "two")))
  }

  test("add a nested column in the middle of the middle struct column") {
    testAdd(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 1 c1, named_struct('c3', 2, 'c7', 5, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      Seq(
        Row(1, Row(2, null, Row(3, 4)), "one"),
        Row(1, Row(2, 5, Row(3, 4)), "two")))
  }
}

/**
 * Hide a nested column.
 */
trait HideNestedColumnTest extends ReadSchemaTest {

  private def testHide(df1: DataFrame, df2: DataFrame, df3: DataFrame, expectedRows: Seq[Row]) = {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      val dir1 = s"$path${File.separator}part=one"
      val dir2 = s"$path${File.separator}part=two"
      val dir3 = s"$path${File.separator}part=three"

      df1.write.format(format).options(options).save(dir1)
      df2.write.format(format).options(options).save(dir2)
      df3.write.format(format).options(options).save(dir3)

      val df = spark.read
        .schema(df1.schema)
        .format(format)
        .options(options)
        .load(path)

      checkAnswer(df, expectedRows)
    }
  }

  test("hide a nested column at the end of the leaf struct column") {
    testHide(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 5 c1, named_struct('c3', 6, 'c4', named_struct('c5', 7, 'c6', 8, 'c7', 9)) c2"),
      sql("SELECT 0 c1, named_struct('c3', 1, 'c4', named_struct('c5', 2, 'c6', 3, 'c8', 4)) c2"),
      Seq(
        Row(1, Row(2, Row(3, 4)), "one"),
        Row(5, Row(6, Row(7, 8)), "two"),
        Row(0, Row(1, Row(2, 3)), "three")))
  }

  test("hide a nested column in the middle of the leaf struct column") {
    testHide(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 5 c1, named_struct('c3', 6, 'c4', named_struct('c5', 7, 'c7', 8, 'c6', 9)) c2"),
      sql("SELECT 0 c1, named_struct('c3', 1, 'c4', named_struct('c7', 2, 'c5', 3, 'c6', 4)) c2"),
      Seq(
        Row(1, Row(2, Row(3, 4)), "one"),
        Row(5, Row(6, Row(7, 9)), "two"),
        Row(0, Row(1, Row(3, 4)), "three")))
  }

  test("hide a nested column at the end of the middle struct column") {
    testHide(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 5 c1, named_struct('c3', 6, 'c4', named_struct('c5', 7, 'c6', 8), 'c7', 9) c2"),
      sql("SELECT 0 c1, named_struct('c3', 1, 'c4', named_struct('c5', 2, 'c6', 3), 'c8', 4) c2"),
      Seq(
        Row(1, Row(2, Row(3, 4)), "one"),
        Row(5, Row(6, Row(7, 8)), "two"),
        Row(0, Row(1, Row(2, 3)), "three")))
  }

  test("hide a nested column in the middle of the middle struct column") {
    testHide(
      sql("SELECT 1 c1, named_struct('c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      sql("SELECT 5 c1, named_struct('c3', 6, 'c7', 7, 'c4', named_struct('c5', 8, 'c6', 9)) c2"),
      sql("SELECT 0 c1, named_struct('c7', 1, 'c3', 2, 'c4', named_struct('c5', 3, 'c6', 4)) c2"),
      Seq(
        Row(1, Row(2, Row(3, 4)), "one"),
        Row(5, Row(6, Row(8, 9)), "two"),
        Row(0, Row(2, Row(3, 4)), "three")))
  }
}

