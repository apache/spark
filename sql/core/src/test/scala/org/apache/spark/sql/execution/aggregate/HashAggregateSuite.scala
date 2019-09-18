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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class HashAggregateSuite extends SharedSparkSession {

  import testImplicits._

  test("SPARK-29140 HashAggregateExec aggregating binary type doesn't break codegen compilation") {
    val withDistinct = countDistinct($"c1")

    val schema = new StructType().add("c1", BinaryType, nullable = true)
    val schemaWithId = StructType(StructField("id", IntegerType, nullable = false) +: schema.fields)

    withSQLConf(
        SQLConf.CODEGEN_SPLIT_AGGREGATE_FUNC.key -> "true",
        SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> "1") {
      val emptyRows = spark.sparkContext.parallelize(Seq.empty[Row], 1)
      val aggDf = spark.createDataFrame(emptyRows, schemaWithId)
        .groupBy($"id" % 10 as "group")
        .agg(withDistinct)
        .orderBy("group")
      aggDf.collect().toSeq
    }
  }
}
