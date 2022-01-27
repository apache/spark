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

package org.apache.spark.sql.execution.datasources.csv

import java.io.File

import org.apache.spark.sql.{AnalysisException, FileBasedDataSourceSuiteBase, Row}
import org.apache.spark.sql.types.{StructField, StructType, TestUDT}

class CSVDataSourceSuite extends FileBasedDataSourceSuiteBase {

  import testImplicits._

  override protected def format: String = "csv"

  override def excluded: Seq[String] =
    Seq("SPARK-15474: Write and read back non-empty schema with empty dataframe",
      "SPARK-32889: column name supports special characters",
      "Spark native readers should respect spark.sql.caseSensitive",
      "Return correct results when data columns overlap with partition columns (nested data)",
      "SPARK-31116: Select nested schema with case insensitive mode",
      "test casts pushdown on orc/parquet for integral types")

  // Unsupported data types of csv, json, orc, and parquet are as follows;
  //  csv -> R/W: Null, Array, Map, Struct
  //  json -> R/W: Interval
  //  orc -> R/W: Interval, W: Null
  //  parquet -> R/W: Interval, Null
  test("SPARK-24204 error handling for unsupported Array/Map/Struct types - csv") {
    withTempDir { dir =>
      val csvDir = new File(dir, "csv").getCanonicalPath
      var msg = intercept[AnalysisException] {
        Seq((1, "Tesla")).toDF("a", "b").selectExpr("struct(a, b)").write.csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support struct<a:int,b:string> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType.fromDDL("a struct<b: Int>")
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support struct<b:int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, Map("Tesla" -> 3))).toDF("id", "cars").write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support map<string,int> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType.fromDDL("a map<int, int>")
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support map<int,int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, Array("Tesla", "Chevy", "Ford"))).toDF("id", "brands")
          .write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<string> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType.fromDDL("a array<int>")
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support array<int> data type"))

      msg = intercept[AnalysisException] {
        Seq((1, new TestUDT.MyDenseVector(Array(0.25, 2.25, 4.25)))).toDF("id", "vectors")
          .write.mode("overwrite").csv(csvDir)
      }.getMessage
      assert(msg.contains("CSV data source does not support array<double> data type"))

      msg = intercept[AnalysisException] {
        val schema = StructType(StructField("a", new TestUDT.MyDenseVectorUDT(), true) :: Nil)
        spark.range(1).write.mode("overwrite").csv(csvDir)
        spark.read.schema(schema).csv(csvDir).collect()
      }.getMessage
      assert(msg.contains("CSV data source does not support array<double> data type."))
    }
  }


  test("SPARK-35669: special char in CSV header with filter pushdown") {
    withTempPath { path =>
      val pathStr = path.getCanonicalPath
      Seq("a / b,a`b", "v1,v2").toDF().coalesce(1).write.text(pathStr)
      val df = spark.read.option("header", true).csv(pathStr)
        .where($"a / b".isNotNull and $"`a``b`".isNotNull)
      checkAnswer(df, Row("v1", "v2"))
    }
  }
}
