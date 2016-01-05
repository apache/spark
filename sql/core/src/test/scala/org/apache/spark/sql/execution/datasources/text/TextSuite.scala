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

package org.apache.spark.sql.execution.datasources.text

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.util.Utils


class TextSuite extends QueryTest with SharedSQLContext {

  test("reading text file") {
    verifyFrame(sqlContext.read.format("text").load(testFile))
  }

  test("SQLContext.read.text() API") {
    verifyFrame(sqlContext.read.text(testFile))
  }

  test("SPARK-12562 verify write.text() can handle column name beyond `value`") {
    val df = sqlContext.read.text(testFile).withColumnRenamed("value", "adwrasdf")

    val tempFile = Utils.createTempDir()
    tempFile.delete()
    df.write.text(tempFile.getCanonicalPath)
    verifyFrame(sqlContext.read.text(tempFile.getCanonicalPath))

    Utils.deleteRecursively(tempFile)
  }

  test("error handling for invalid schema") {
    val tempFile = Utils.createTempDir()
    tempFile.delete()

    val df = sqlContext.range(2)
    intercept[AnalysisException] {
      df.write.text(tempFile.getCanonicalPath)
    }

    intercept[AnalysisException] {
      sqlContext.range(2).select(df("id"), df("id") + 1).write.text(tempFile.getCanonicalPath)
    }
  }

  private def testFile: String = {
    Thread.currentThread().getContextClassLoader.getResource("text-suite.txt").toString
  }

  /** Verifies data and schema. */
  private def verifyFrame(df: DataFrame): Unit = {
    // schema
    assert(df.schema == new StructType().add("value", StringType))

    // verify content
    val data = df.collect()
    assert(data(0) == Row("This is a test file for the text data source"))
    assert(data(1) == Row("1+1"))
    // non ascii characters are not allowed in the code, so we disable the scalastyle here.
    // scalastyle:off
    assert(data(2) == Row("数据砖头"))
    // scalastyle:on
    assert(data(3) == Row("\"doh\""))
    assert(data.length == 4)
  }
}
