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

import _root_.java.io.File

/* Implicits */
import org.apache.spark.sql.test.TestSQLContext._

class InsertIntoSuite extends QueryTest {
  TestData // Initialize TestData
  import TestData._

  test("insertInto() created parquet file") {
    val testFilePath = File.createTempFile("sparkSql", "pqt")
    testFilePath.delete()
    testFilePath.deleteOnExit()
    val testFile = createParquetFile[TestData](testFilePath.getCanonicalPath)
    testFile.registerAsTable("createAndInsertTest")

    // Add some data.
    testData.insertInto("createAndInsertTest")

    // Make sure its there for a new instance of parquet file.
    checkAnswer(
      parquetFile(testFilePath.getCanonicalPath),
      testData.collect().toSeq
    )

    // Make sure the registered table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertTest"),
      testData.collect().toSeq
    )

    // Add more data.
    testData.insertInto("createAndInsertTest")

    // Make sure all data is there for a new instance of parquet file.
    checkAnswer(
      parquetFile(testFilePath.getCanonicalPath),
      testData.collect().toSeq ++ testData.collect().toSeq
    )

    // Make sure the registered table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertTest"),
      testData.collect().toSeq ++ testData.collect().toSeq
    )

    // Now overwrite.
    testData.insertInto("createAndInsertTest", overwrite = true)

    // Make sure its there for a new instance of parquet file.
    checkAnswer(
      parquetFile(testFilePath.getCanonicalPath),
      testData.collect().toSeq
    )

    // Make sure the registered table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertTest"),
      testData.collect().toSeq
    )

    testFilePath.delete()
  }

  test("INSERT INTO parquet table") {
    val testFilePath = File.createTempFile("sparkSql", "pqt")
    testFilePath.delete()
    testFilePath.deleteOnExit()
    val testFile = createParquetFile[TestData](testFilePath.getCanonicalPath)
    testFile.registerAsTable("createAndInsertSQLTest")

    sql("INSERT INTO createAndInsertSQLTest SELECT * FROM testData")

    // Make sure its there for a new instance of parquet file.
    checkAnswer(
      parquetFile(testFilePath.getCanonicalPath),
      testData.collect().toSeq
    )

    // Make sure the registered table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertSQLTest"),
      testData.collect().toSeq
    )

    // Append more data.
    sql("INSERT INTO createAndInsertSQLTest SELECT * FROM testData")

    // Make sure all data is there for a new instance of parquet file.
    checkAnswer(
      parquetFile(testFilePath.getCanonicalPath),
      testData.collect().toSeq ++ testData.collect().toSeq
    )

    // Make sure the registered table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertSQLTest"),
      testData.collect().toSeq ++ testData.collect().toSeq
    )

    sql("INSERT OVERWRITE INTO createAndInsertSQLTest SELECT * FROM testData")

    // Make sure its there for a new instance of parquet file.
    checkAnswer(
      parquetFile(testFilePath.getCanonicalPath),
      testData.collect().toSeq
    )

    // Make sure the registered table has also been updated.
    checkAnswer(
      sql("SELECT * FROM createAndInsertSQLTest"),
      testData.collect().toSeq
    )

    testFilePath.delete()
  }

  test("Double create fails when allowExisting = false") {
    val testFilePath = File.createTempFile("sparkSql", "pqt")
    testFilePath.delete()
    testFilePath.deleteOnExit()
    val testFile = createParquetFile[TestData](testFilePath.getCanonicalPath)

    intercept[RuntimeException] {
      createParquetFile[TestData](testFilePath.getCanonicalPath, allowExisting = false)
    }

    testFilePath.delete()
  }

  test("Double create does not fail when allowExisting = true") {
    val testFilePath = File.createTempFile("sparkSql", "pqt")
    testFilePath.delete()
    testFilePath.deleteOnExit()
    val testFile = createParquetFile[TestData](testFilePath.getCanonicalPath)

    createParquetFile[TestData](testFilePath.getCanonicalPath, allowExisting = true)

    testFilePath.delete()
  }
}
