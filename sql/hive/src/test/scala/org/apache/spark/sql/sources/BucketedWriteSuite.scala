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

package org.apache.spark.sql.sources

import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.{AnalysisException, QueryTest}

class BucketedWriteSuite extends QueryTest with SQLTestUtils with TestHiveSingleton {
  import testImplicits._

  test("bucketed by non-existing column") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "k").saveAsTable("tt"))
  }

  test("numBuckets not greater than 0") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.bucketBy(0, "i").saveAsTable("tt"))
  }

  test("specify sorting columns without bucketing columns") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.sortBy("j").saveAsTable("tt"))
  }

  test("sorting by non-orderable column") {
    val df = Seq("a" -> Map(1 -> 1), "b" -> Map(2 -> 2)).toDF("i", "j")
    intercept[AnalysisException](df.write.bucketBy(2, "i").sortBy("j").saveAsTable("tt"))
  }

  test("write bucketed data to non-hive-table or existing hive table") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").parquet("/tmp/path"))
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").json("/tmp/path"))
    intercept[IllegalArgumentException](df.write.bucketBy(2, "i").insertInto("tt"))
  }

  test("write bucketed data") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    withTable("bucketedTable") {
      df.write.partitionBy("i").bucketBy(8, "j").saveAsTable("bucketedTable")
    }
  }

  test("write bucketed data without partitioning") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    withTable("bucketedTable") {
      df.write.bucketBy(8, "i").sortBy("j").saveAsTable("bucketedTable")
    }
  }
}
