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

import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType

class ExplainSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  /**
   * Runs the plan and makes sure the plans contains all of the keywords.
   */
  private def checkKeywordsExistsInExplain(df: DataFrame, keywords: String*): Unit = {
    val output = new java.io.ByteArrayOutputStream()
    Console.withOut(output) {
      df.explain(extended = false)
    }
    for (key <- keywords) {
      assert(output.toString.contains(key))
    }
  }

  test("SPARK-23034 show rdd names in RDD scan nodes (Dataset)") {
    val rddWithName = spark.sparkContext.parallelize(Row(1, "abc") :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName, StructType.fromDDL("c0 int, c1 string"))
    checkKeywordsExistsInExplain(df, keywords = "Scan ExistingRDD testRdd")
  }

  test("SPARK-23034 show rdd names in RDD scan nodes (DataFrame)") {
    val rddWithName = spark.sparkContext.parallelize(ExplainSingleData(1) :: Nil).setName("testRdd")
    val df = spark.createDataFrame(rddWithName)
    checkKeywordsExistsInExplain(df, keywords = "Scan testRdd")
  }

  test("SPARK-24850 InMemoryRelation string representation does not include cached plan") {
    val df = Seq(1).toDF("a").cache()
    checkKeywordsExistsInExplain(df,
      keywords = "InMemoryRelation", "StorageLevel(disk, memory, deserialized, 1 replicas)")
  }
}

case class ExplainSingleData(id: Int)
