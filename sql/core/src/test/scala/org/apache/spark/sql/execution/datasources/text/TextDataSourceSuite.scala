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

import java.io.File

import org.apache.spark.sql.{AnalysisException, FileDataSourceSuiteBase}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructField, StructType}

class TextDataSourceSuite extends FileDataSourceSuiteBase {
  import testImplicits._

  override protected def format: String = "text"

  override def excluded: Seq[String] =
    Seq("SPARK-23072: Write and read back unicode column names",
      "SPARK-15474: Write and read back non-empty schema with empty dataframe",
      "SPARK-23148: read files containing special characters with multiline enabled",
      "SPARK-32889: column name supports special characters",
      "Spark native readers should respect spark.sql.caseSensitive",
      "SPARK-24204: error handling for unsupported Interval data types",
      "SPARK-24204: error handling for unsupported Null data types",
      "Return correct results when data columns overlap with partition columns",
      "Return correct results when data columns overlap with partition columns (nested data)",
      "SPARK-31116: Select nested schema with case insensitive mode",
      "test casts pushdown on orc/parquet for integral types")

  // Text file format only supports string type
  test("SPARK-24691 error handling for unsupported types - text") {
    withTempDir { dir =>
      // write path
      val textDir = new File(dir, "text").getCanonicalPath
      var msg = intercept[AnalysisException] {
        Seq(1).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support int data type"))

      msg = intercept[AnalysisException] {
        Seq(1.2).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support double data type"))

      msg = intercept[AnalysisException] {
        Seq(true).toDF.write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support boolean data type"))

      msg = intercept[AnalysisException] {
        Seq(1).toDF("a").selectExpr("struct(a)").write.text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support struct<a:int> data type"))

      msg = intercept[AnalysisException] {
        Seq((Map("Tesla" -> 3))).toDF("cars").write.mode("overwrite").text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support map<string,int> data type"))

      msg = intercept[AnalysisException] {
        Seq((Array("Tesla", "Chevy", "Ford"))).toDF("brands")
          .write.mode("overwrite").text(textDir)
      }.getMessage
      assert(msg.contains("Text data source does not support array<string> data type"))

      // read path
      Seq("aaa").toDF.write.mode("overwrite").text(textDir)
      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", IntegerType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support int data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", DoubleType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support double data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", BooleanType, true) :: Nil)
        spark.read.schema(schema).text(textDir).collect()
      }.getMessage
      assert(msg.contains("Text data source does not support boolean data type"))
    }
  }

}
