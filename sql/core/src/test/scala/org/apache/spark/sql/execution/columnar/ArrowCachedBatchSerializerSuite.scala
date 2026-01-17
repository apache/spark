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

package org.apache.spark.sql.execution.columnar

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class ArrowCachedBatchSerializerSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[ArrowCachedBatchSerializer].getName)
      .set(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "false")
  }

  test("basic caching with primitive types") {
    val df = Seq(
      (1, 2L, 3.0f, 4.0, "hello"),
      (5, 6L, 7.0f, 8.0, "world"),
      (9, 10L, 11.0f, 12.0, "test")
    ).toDF("a", "b", "c", "d", "e")

    df.cache()
    checkAnswer(df, Seq(
      Row(1, 2L, 3.0f, 4.0, "hello"),
      Row(5, 6L, 7.0f, 8.0, "world"),
      Row(9, 10L, 11.0f, 12.0, "test")
    ))

    // Verify it was actually cached
    assert(df.storageLevel.useMemory)
  }

  test("caching with all primitive types") {
    val df = Seq(
      (true, 1.toByte, 2.toShort, 3, 4L, 5.0f, 6.0),
      (false, 7.toByte, 8.toShort, 9, 10L, 11.0f, 12.0),
      (true, 13.toByte, 14.toShort, 15, 16L, 17.0f, 18.0)
    ).toDF("bool", "byte", "short", "int", "long", "float", "double")

    df.cache()
    checkAnswer(df, Seq(
      Row(true, 1.toByte, 2.toShort, 3, 4L, 5.0f, 6.0),
      Row(false, 7.toByte, 8.toShort, 9, 10L, 11.0f, 12.0),
      Row(true, 13.toByte, 14.toShort, 15, 16L, 17.0f, 18.0)
    ))
  }

  test("caching with null values") {
    val df = Seq(
      (Some(1), Some("a")),
      (None, Some("b")),
      (Some(3), None),
      (None, None)
    ).toDF("num", "str")

    df.cache()
    checkAnswer(df, Seq(
      Row(1, "a"),
      Row(null, "b"),
      Row(3, null),
      Row(null, null)
    ))
  }

  test("caching with date and timestamp types") {
    val date1 = Date.valueOf("2020-01-01")
    val date2 = Date.valueOf("2021-06-15")
    val ts1 = Timestamp.valueOf("2020-01-01 12:00:00")
    val ts2 = Timestamp.valueOf("2021-06-15 15:30:45")

    val df = Seq(
      (date1, ts1),
      (date2, ts2)
    ).toDF("date", "timestamp")

    df.cache()
    checkAnswer(df, Seq(
      Row(date1, ts1),
      Row(date2, ts2)
    ))
  }

  test("caching with decimal types") {
    val df = Seq(
      BigDecimal("123.45"),
      BigDecimal("678.90"),
      BigDecimal("999.99")
    ).toDF("decimal")

    df.cache()
    checkAnswer(df, Seq(
      Row(BigDecimal("123.45")),
      Row(BigDecimal("678.90")),
      Row(BigDecimal("999.99"))
    ))
  }

  test("caching with binary type") {
    val df = Seq(
      "hello".getBytes("UTF-8"),
      "world".getBytes("UTF-8"),
      "test".getBytes("UTF-8")
    ).toDF("binary")

    df.cache()
    val result = df.collect()
    assert(result.length == 3)
    assert(new String(result(0).getAs[Array[Byte]](0), "UTF-8") == "hello")
    assert(new String(result(1).getAs[Array[Byte]](0), "UTF-8") == "world")
    assert(new String(result(2).getAs[Array[Byte]](0), "UTF-8") == "test")
  }

  test("caching with array type") {
    val df = Seq(
      Seq(1, 2, 3),
      Seq(4, 5, 6),
      Seq(7, 8, 9)
    ).toDF("array")

    df.cache()
    checkAnswer(df, Seq(
      Row(Seq(1, 2, 3)),
      Row(Seq(4, 5, 6)),
      Row(Seq(7, 8, 9))
    ))
  }

  test("caching with struct type") {
    val df = Seq(
      (1, ("a", 10)),
      (2, ("b", 20)),
      (3, ("c", 30))
    ).toDF("id", "struct")

    df.cache()
    checkAnswer(df, Seq(
      Row(1, Row("a", 10)),
      Row(2, Row("b", 20)),
      Row(3, Row("c", 30))
    ))
  }

  test("caching with map type") {
    val df = Seq(
      Map("a" -> 1, "b" -> 2),
      Map("c" -> 3, "d" -> 4),
      Map("e" -> 5, "f" -> 6)
    ).toDF("map")

    df.cache()
    checkAnswer(df, Seq(
      Row(Map("a" -> 1, "b" -> 2)),
      Row(Map("c" -> 3, "d" -> 4)),
      Row(Map("e" -> 5, "f" -> 6))
    ))
  }

  test("caching with nested complex types") {
    val df = Seq(
      (1, Seq(("a", Seq(1, 2)), ("b", Seq(3, 4)))),
      (2, Seq(("c", Seq(5, 6)), ("d", Seq(7, 8))))
    ).toDF("id", "nested")

    df.cache()
    checkAnswer(df, Seq(
      Row(1, Seq(Row("a", Seq(1, 2)), Row("b", Seq(3, 4)))),
      Row(2, Seq(Row("c", Seq(5, 6)), Row("d", Seq(7, 8))))
    ))
  }

  test("caching with filter pushdown") {
    val df = (1 to 100).map(i => (i, i * 2, s"str$i")).toDF("a", "b", "c")
    df.cache()

    // This should use cached data with filter
    val filtered = df.filter($"a" > 50)
    checkAnswer(filtered, (51 to 100).map(i => Row(i, i * 2, s"str$i")))

    // Verify cache was used
    assert(filtered.queryExecution.executedPlan.toString.contains("InMemoryTableScan"))
  }

  test("caching with column projection") {
    val df = (1 to 100).map(i => (i, i * 2, i * 3, s"str$i")).toDF("a", "b", "c", "d")
    df.cache()

    // Select subset of columns
    val projected = df.select("a", "c")
    checkAnswer(projected, (1 to 100).map(i => Row(i, i * 3)))

    // Verify cache was used
    assert(projected.queryExecution.executedPlan.toString.contains("InMemoryTableScan"))
  }

  test("caching with multiple batches") {
    withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> "10") {
      val df = (1 to 50).map(i => (i, s"str$i")).toDF("a", "b")
      df.cache()

      checkAnswer(df, (1 to 50).map(i => Row(i, s"str$i")))

      // Verify multiple batches were created
      val plan = df.queryExecution.executedPlan
      val inMemoryScan = plan.collectFirst {
        case scan: InMemoryTableScanExec => scan
      }
      assert(inMemoryScan.isDefined)
    }
  }

  test("uncache and recache") {
    val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")

    // Cache
    df.cache()
    checkAnswer(df, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    assert(df.storageLevel.useMemory)

    // Uncache
    df.unpersist()
    assert(!df.storageLevel.useMemory)

    // Recache
    df.cache()
    checkAnswer(df, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    assert(df.storageLevel.useMemory)
  }

  test("cache with aggregation") {
    val df = Seq(
      ("a", 1),
      ("b", 2),
      ("a", 3),
      ("b", 4),
      ("a", 5)
    ).toDF("key", "value")

    df.cache()

    val agg = df.groupBy("key").sum("value")
    checkAnswer(agg, Seq(Row("a", 9), Row("b", 6)))
  }

  test("cache with join") {
    val df1 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value1")
    val df2 = Seq((1, "x"), (2, "y"), (3, "z")).toDF("id", "value2")

    df1.cache()
    df2.cache()

    val joined = df1.join(df2, "id")
    checkAnswer(joined, Seq(
      Row(1, "a", "x"),
      Row(2, "b", "y"),
      Row(3, "c", "z")
    ))
  }

  test("vectorized reader enabled") {
    withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true") {
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
      df.cache()

      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))

      // Verify vectorized reader is used
      val plan = df.queryExecution.executedPlan
      val inMemoryScan = plan.collectFirst {
        case scan: InMemoryTableScanExec => scan
      }
      assert(inMemoryScan.isDefined)
      assert(inMemoryScan.get.supportsColumnar)
    }
  }

  test("compression codec - none") {
    withSQLConf(SQLConf.ARROW_EXECUTION_COMPRESSION_CODEC.key -> "none") {
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
      df.cache()
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("compression codec - zstd") {
    withSQLConf(SQLConf.ARROW_EXECUTION_COMPRESSION_CODEC.key -> "zstd") {
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
      df.cache()
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("compression codec - lz4") {
    withSQLConf(SQLConf.ARROW_EXECUTION_COMPRESSION_CODEC.key -> "lz4") {
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
      df.cache()
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("large dataset") {
    val df = (1 to 10000).map(i => (i, i * 2, s"string$i")).toDF("a", "b", "c")
    df.cache()

    checkAnswer(
      df.filter($"a" > 9000),
      (9001 to 10000).map(i => Row(i, i * 2, s"string$i"))
    )
  }

  test("empty dataset") {
    val df = Seq.empty[(Int, String)].toDF("id", "value")
    df.cache()
    checkAnswer(df, Seq.empty[Row])
  }

  test("single row") {
    val df = Seq((1, "single")).toDF("id", "value")
    df.cache()
    checkAnswer(df, Seq(Row(1, "single")))
  }

  test("cache table command") {
    withTempView("test_table") {
      Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
        .createOrReplaceTempView("test_table")

      sql("CACHE TABLE test_table")

      checkAnswer(
        sql("SELECT * FROM test_table"),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))
      )

      sql("UNCACHE TABLE test_table")
    }
  }

  test("columnar batch from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = (1 to 100).map(i => (i, i * 2, s"str$i")).toDF("a", "b", "c")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, (1 to 100).map(i => Row(i, i * 2, s"str$i")))
    }
  }
}
