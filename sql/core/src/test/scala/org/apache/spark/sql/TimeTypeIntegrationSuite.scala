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

import java.time.LocalTime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StringType, TimeType}

/**
 * Comprehensive integration tests for TIME data type across various data sources
 * and SQL operations, covering changes in the vandana-ibm-3.5.4 branch.
 */
class TimeTypeIntegrationSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  private def checkTimeResults(df: DataFrame, expected: Seq[LocalTime]): Unit = {
    val result = df.collect().map(_.get(0).asInstanceOf[LocalTime])
    assert(result.toSeq === expected)
  }

  test("Integration: Parquet save and load") {
    val data = Seq(LocalTime.of(10, 30), LocalTime.of(14, 0))
    val df = data.map(Tuple1(_)).toDF("t")
    withTempPath { path =>
      df.write.parquet(path.getCanonicalPath)
      val loaded = spark.read.parquet(path.getCanonicalPath)
      checkTimeResults(loaded.orderBy("t"), data.sorted)
    }
  }

  test("Integration: JSON save and load") {
    val data = Seq(LocalTime.of(10, 30), LocalTime.of(14, 0))
    val df = data.map(Tuple1(_)).toDF("t")
    withTempPath { path =>
      df.write.json(path.getCanonicalPath)
      // JSON requires schema for TimeType as it's not inferred as TimeType by default
      val loaded = spark.read.schema(df.schema).json(path.getCanonicalPath)
      checkTimeResults(loaded.orderBy("t"), data.sorted)
    }
  }

  test("Integration: CSV save and load") {
    val data = Seq(LocalTime.of(10, 30), LocalTime.of(14, 0))
    val df = data.map(Tuple1(_)).toDF("t")
    withTempPath { path =>
      df.write.option("header", "true").csv(path.getCanonicalPath)
      // CSV requires schema for TimeType
      val loaded = spark.read.option("header", "true").schema(df.schema).csv(path.getCanonicalPath)
      checkTimeResults(loaded.orderBy("t"), data.sorted)
    }
  }

  test("Integration: ORC save and load") {
    val data = Seq(LocalTime.of(10, 30), LocalTime.of(14, 0))
    val df = data.map(Tuple1(_)).toDF("t")
    withTempPath { path =>
      df.write.orc(path.getCanonicalPath)
      val loaded = spark.read.orc(path.getCanonicalPath)
      checkTimeResults(loaded.orderBy("t"), data.sorted)
    }
  }

  test("Integration: Partitioning by TIME column") {
    val data = Seq((1, LocalTime.of(10, 0)), (2, LocalTime.of(14, 0)))
    val df = data.toDF("id", "t")
    withTempPath { path =>
      df.write.partitionBy("t").parquet(path.getCanonicalPath)
      val loaded = spark.read.parquet(path.getCanonicalPath)
      assert(loaded.filter($"t" === LocalTime.of(10, 0)).count() === 1)
      assert(loaded.filter($"t" === LocalTime.of(14, 0)).count() === 1)
    }
  }

  test("Integration: Join and Aggregation with TIME column") {
    val df1 = Seq((1, LocalTime.of(10, 0)), (2, LocalTime.of(14, 0))).toDF("id", "t1")
    val df2 = Seq((1, LocalTime.of(10, 0)), (3, LocalTime.of(18, 0))).toDF("id", "t2")

    // Join
    val joined = df1.join(df2, $"t1" === $"t2")
    assert(joined.count() === 1)
    assert(joined.select("t1").collect().head.get(0) === LocalTime.of(10, 0))

    // Aggregation
    val agg = df1.union(Seq((3, LocalTime.of(10, 0))).toDF("id", "t1"))
      .groupBy("t1").count()
    val results = agg.collect().map(r => (r.get(0).asInstanceOf[LocalTime], r.getLong(1))).toMap
    assert(results(LocalTime.of(10, 0)) === 2L)
    assert(results(LocalTime.of(14, 0)) === 1L)
  }

  test("Integration: Hive table with TIME column") {
    if (spark.conf.get("spark.sql.catalogImplementation") == "hive") {
      val tableName = "hive_time_test"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName (t TIME) STORED AS PARQUET")
        sql(s"INSERT INTO $tableName VALUES (TIME '10:30:00')")
        val df = sql(s"SELECT * FROM $tableName")
        checkTimeResults(df, Seq(LocalTime.of(10, 30)))
      }
    }
  }

  test("Integration: Bucket by TIME column") {
    val data = (1 to 10).map(i => (i, LocalTime.of(i % 24, 0)))
    val df = data.toDF("id", "t")
    withTable("bucketed_time") {
      df.write.mode("overwrite").bucketBy(4, "t").saveAsTable("bucketed_time")
      val loaded = spark.table("bucketed_time")
      assert(loaded.count() === 10)
      checkAnswer(loaded.filter($"t" === LocalTime.of(5, 0)), Row(5, LocalTime.of(5, 0)))
    }
  }

  test("Integration: Null values handling") {
    val data = Seq(Some(LocalTime.of(10, 30)), None)
    val df = data.map(Tuple1(_)).toDF("t")
    withTempPath { path =>
      df.write.parquet(path.getCanonicalPath)
      val loaded = spark.read.parquet(path.getCanonicalPath)
      val result = loaded.collect().map(r => Option(r.get(0)).asInstanceOf[Option[LocalTime]])
      assert(result.toSet === data.toSet)
    }
  }

  test("Integration: Edge cases - midnight and end of day") {
    val data = Seq(LocalTime.MIDNIGHT, LocalTime.of(23, 59, 59, 999999000))
    val df = data.map(Tuple1(_)).toDF("t")
    withTempPath { path =>
      df.write.parquet(path.getCanonicalPath)
      val loaded = spark.read.parquet(path.getCanonicalPath)
      checkTimeResults(loaded.orderBy("t"), data.sorted)
    }
  }

  test("Integration: Casting between TIME and other types") {
    val df = Seq("10:30:00", "14:00:00.123456").toDF("t_str")
    val casted = df.select($"t_str".cast(TimeType).as("t"))
    checkTimeResults(casted.orderBy("t"),
      Seq(LocalTime.of(10, 30), LocalTime.of(14, 0, 0, 123456000)))

    val timeDf = Seq(LocalTime.of(10, 30)).map(Tuple1(_)).toDF("t")
    val stringCasted = timeDf.select($"t".cast(StringType).as("t_str"))
    // TimeUtils.timeToStringForCast omits microseconds if zero
    assert(stringCasted.collect().head.getString(0) === "10:30:00")
  }

  test("Integration: Min/Max Aggregations") {
    val data = Seq(LocalTime.of(10, 30), LocalTime.of(14, 0), LocalTime.of(8, 15))
    val df = data.map(Tuple1(_)).toDF("t")
    val agg = df.select(min("t"), max("t"))
    val result = agg.collect().head
    assert(result.get(0) === LocalTime.of(8, 15))
    assert(result.get(1) === LocalTime.of(14, 0))
  }
}
