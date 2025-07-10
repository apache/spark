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

package org.apache.spark.sql.connect

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession}
import org.apache.spark.sql.functions.{concat, lit, when}

class DataFrameSuite extends QueryTest with RemoteSparkSession {

  test("drop") {
    val sparkSession = spark
    import sparkSession.implicits._

    val df1 = Seq[(String, String, String)](("a", "b", "c")).toDF("colA", "colB", "colC")

    val df2 = Seq[(String, String, String)](("c", "d", "e")).toDF("colC", "colD", "colE")

    val df3 = df1
      .join(df2, df1.col("colC") === df2.col("colC"))
      .withColumn(
        "colB",
        when(df1.col("colB") === "b", concat(df1.col("colB").cast("string"), lit("x")))
          .otherwise(df1.col("colB")))

    val df4 = df3.drop(df1.col("colB"))

    assert(df4.columns === Array("colA", "colB", "colC", "colC", "colD", "colE"))
    assert(df4.count() === 1)
  }

  test("lazy column validation") {
    val session = spark
    import session.implicits._

    val df1 = Seq(1 -> "y").toDF("a", "y")
    val df2 = Seq(1 -> "x").toDF("a", "x")
    val df3 = df1.join(df2, df1("a") === df2("a"))
    val df4 = df3.select(df1("x")) // <- No exception here

    intercept[AnalysisException] { df4.schema }
  }
}
