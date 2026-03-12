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
package org.apache.spark.sql.execution.arrow

import java.io.File

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class ArrowFileReadWriteSuite extends QueryTest with SharedSparkSession {

  private var tempDataPath: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDataPath = Utils.createTempDir(namePrefix = "arrowFileReadWrite").getAbsolutePath
  }

  test("simple") {
    val df = spark.range(0, 100, 1, 10).select(
      col("id"),
      lit(1).alias("int"),
      lit(2L).alias("long"),
      lit(3.0).alias("double"),
      lit("a string").alias("str"),
      lit(Array(1.0, 2.0, Double.NaN, Double.NegativeInfinity)).alias("arr"))

    val path = new File(tempDataPath, "simple.arrowfile").toPath
    ArrowFileReadWrite.save(df, path)

    val df2 = ArrowFileReadWrite.load(spark, path)
    checkAnswer(df, df2)
  }

  test("empty dataframe") {
    val df = spark.range(0).withColumn("v", lit(1))
    assert(df.count() === 0)

    val path = new File(tempDataPath, "empty.arrowfile").toPath
    ArrowFileReadWrite.save(df, path)

    val df2 = ArrowFileReadWrite.load(spark, path)
    checkAnswer(df, df2)
  }
}
