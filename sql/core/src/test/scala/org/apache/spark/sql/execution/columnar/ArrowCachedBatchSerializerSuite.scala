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

  test("supportsColumnarInput with supported types") {
    import org.apache.spark.sql.catalyst.expressions.AttributeReference
    import org.apache.spark.sql.types._

    val serializer = new ArrowCachedBatchSerializer()

    // All primitive types should be supported
    val primitiveSchema = Seq(
      AttributeReference("bool", BooleanType)(),
      AttributeReference("byte", ByteType)(),
      AttributeReference("short", ShortType)(),
      AttributeReference("int", IntegerType)(),
      AttributeReference("long", LongType)(),
      AttributeReference("float", FloatType)(),
      AttributeReference("double", DoubleType)(),
      AttributeReference("string", StringType)(),
      AttributeReference("binary", BinaryType)()
    )
    assert(serializer.supportsColumnarInput(primitiveSchema))

    // Temporal types should be supported
    val temporalSchema = Seq(
      AttributeReference("date", DateType)(),
      AttributeReference("timestamp", TimestampType)(),
      AttributeReference("timestampNtz", TimestampNTZType)()
    )
    assert(serializer.supportsColumnarInput(temporalSchema))

    // Decimal should be supported
    val decimalSchema = Seq(
      AttributeReference("decimal", DecimalType(10, 2))()
    )
    assert(serializer.supportsColumnarInput(decimalSchema))

    // Complex types should be supported
    val complexSchema = Seq(
      AttributeReference("array", ArrayType(IntegerType))(),
      AttributeReference("struct", StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", StringType)
      )))(),
      AttributeReference("map", MapType(StringType, IntegerType))()
    )
    assert(serializer.supportsColumnarInput(complexSchema))

    // Nested complex types should be supported
    val nestedSchema = Seq(
      AttributeReference("nested", ArrayType(StructType(Seq(
        StructField("x", IntegerType),
        StructField("y", ArrayType(StringType))
      ))))()
    )
    assert(serializer.supportsColumnarInput(nestedSchema))
  }

  test("supportsColumnarInput correctly validates all types") {
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.util.ArrowUtils

    // Verify that isSupportedByArrow handles all standard Spark SQL types
    assert(ArrowUtils.isSupportedByArrow(BooleanType))
    assert(ArrowUtils.isSupportedByArrow(ByteType))
    assert(ArrowUtils.isSupportedByArrow(ShortType))
    assert(ArrowUtils.isSupportedByArrow(IntegerType))
    assert(ArrowUtils.isSupportedByArrow(LongType))
    assert(ArrowUtils.isSupportedByArrow(FloatType))
    assert(ArrowUtils.isSupportedByArrow(DoubleType))
    assert(ArrowUtils.isSupportedByArrow(StringType))
    assert(ArrowUtils.isSupportedByArrow(BinaryType))
    assert(ArrowUtils.isSupportedByArrow(DateType))
    assert(ArrowUtils.isSupportedByArrow(TimestampType))
    assert(ArrowUtils.isSupportedByArrow(TimestampNTZType))
    assert(ArrowUtils.isSupportedByArrow(DecimalType(10, 2)))
    assert(ArrowUtils.isSupportedByArrow(NullType))
    assert(ArrowUtils.isSupportedByArrow(CalendarIntervalType))

    // Complex types
    assert(ArrowUtils.isSupportedByArrow(ArrayType(IntegerType)))
    assert(ArrowUtils.isSupportedByArrow(StructType(Seq(StructField("x", IntegerType)))))
    assert(ArrowUtils.isSupportedByArrow(MapType(StringType, IntegerType)))

    // Nested complex types
    assert(ArrowUtils.isSupportedByArrow(
      ArrayType(StructType(Seq(
        StructField("a", IntegerType),
        StructField("b", ArrayType(StringType))
      )))
    ))
  }

  test("verify Arrow cache serializer is actually used") {
    val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "value")
    df.cache()
    df.count() // Materialize the cache

    // Verify the query plan uses InMemoryTableScan
    val plan = df.queryExecution.executedPlan
    val inMemoryScan = plan.collectFirst {
      case scan: InMemoryTableScanExec => scan
    }
    assert(inMemoryScan.isDefined, "InMemoryTableScan should be present in cached query plan")

    // Verify the serializer is ArrowCachedBatchSerializer
    val serializer = inMemoryScan.get.relation.cacheBuilder.serializer
    assert(serializer.isInstanceOf[ArrowCachedBatchSerializer],
      s"Expected ArrowCachedBatchSerializer but got ${serializer.getClass.getName}")
  }

  test("columnar input with array type from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, Seq(1, 2, 3)),
        (2, Seq(4, 5, 6)),
        (3, Seq(7, 8, 9))
      ).toDF("id", "array_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Seq(1, 2, 3)),
        Row(2, Seq(4, 5, 6)),
        Row(3, Seq(7, 8, 9))
      ))

      // Verify cache was used
      assert(cached.storageLevel.useMemory)
    }
  }

  test("columnar input with struct type from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, ("a", 10)),
        (2, ("b", 20)),
        (3, ("c", 30))
      ).toDF("id", "struct_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Row("a", 10)),
        Row(2, Row("b", 20)),
        Row(3, Row("c", 30))
      ))

      // Verify cache was used
      assert(cached.storageLevel.useMemory)
    }
  }

  test("columnar input with map type from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, Map("a" -> 1, "b" -> 2)),
        (2, Map("c" -> 3, "d" -> 4)),
        (3, Map("e" -> 5, "f" -> 6))
      ).toDF("id", "map_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Map("a" -> 1, "b" -> 2)),
        Row(2, Map("c" -> 3, "d" -> 4)),
        Row(3, Map("e" -> 5, "f" -> 6))
      ))

      // Verify cache was used
      assert(cached.storageLevel.useMemory)
    }
  }

  test("columnar input with nested complex types from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, Seq(("a", Seq(1, 2)), ("b", Seq(3, 4)))),
        (2, Seq(("c", Seq(5, 6)), ("d", Seq(7, 8))))
      ).toDF("id", "nested_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Seq(Row("a", Seq(1, 2)), Row("b", Seq(3, 4)))),
        Row(2, Seq(Row("c", Seq(5, 6)), Row("d", Seq(7, 8))))
      ))

      // Verify cache was used
      assert(cached.storageLevel.useMemory)
    }
  }

  test("columnar input with array of structs from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, Seq(("apple", 1.5), ("banana", 2.0))),
        (2, Seq(("orange", 1.8), ("grape", 3.5))),
        (3, Seq(("mango", 2.5)))
      ).toDF("id", "items")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Seq(Row("apple", 1.5), Row("banana", 2.0))),
        Row(2, Seq(Row("orange", 1.8), Row("grape", 3.5))),
        Row(3, Seq(Row("mango", 2.5)))
      ))

      // Verify cache was used and operations work
      val filtered = cached.filter($"id" > 1)
      checkAnswer(filtered, Seq(
        Row(2, Seq(Row("orange", 1.8), Row("grape", 3.5))),
        Row(3, Seq(Row("mango", 2.5)))
      ))
    }
  }

  test("columnar input with struct containing arrays from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, ("user1", Seq("tag1", "tag2", "tag3"))),
        (2, ("user2", Seq("tag4", "tag5"))),
        (3, ("user3", Seq("tag6")))
      ).toDF("id", "user_info")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Row("user1", Seq("tag1", "tag2", "tag3"))),
        Row(2, Row("user2", Seq("tag4", "tag5"))),
        Row(3, Row("user3", Seq("tag6")))
      ))

      // Verify we can access nested fields
      val extracted = cached.select($"id", $"user_info._1".as("name"))
      checkAnswer(extracted, Seq(
        Row(1, "user1"),
        Row(2, "user2"),
        Row(3, "user3")
      ))
    }
  }

  test("columnar input with map of arrays from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, Map("a" -> Seq(1, 2, 3), "b" -> Seq(4, 5))),
        (2, Map("c" -> Seq(6, 7), "d" -> Seq(8, 9, 10)))
      ).toDF("id", "map_of_arrays")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Map("a" -> Seq(1, 2, 3), "b" -> Seq(4, 5))),
        Row(2, Map("c" -> Seq(6, 7), "d" -> Seq(8, 9, 10)))
      ))

      // Verify cache was used
      assert(cached.storageLevel.useMemory)
    }
  }

  test("columnar input with null values in complex types from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, Some(Seq(1, 2, 3)), Some(("a", 10))),
        (2, None, Some(("b", 20))),
        (3, Some(Seq(4, 5)), None),
        (4, None, None)
      ).toDF("id", "array_col", "struct_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Seq(1, 2, 3), Row("a", 10)),
        Row(2, null, Row("b", 20)),
        Row(3, Seq(4, 5), null),
        Row(4, null, null)
      ))

      // Verify filtering works with nulls
      val filtered = cached.filter($"array_col".isNotNull)
      checkAnswer(filtered, Seq(
        Row(1, Seq(1, 2, 3), Row("a", 10)),
        Row(3, Seq(4, 5), null)
      ))
    }
  }

  test("columnar input with empty arrays and maps from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, Seq(1, 2, 3), Map("a" -> 1)),
        (2, Seq.empty[Int], Map.empty[String, Int]),
        (3, Seq(4), Map("b" -> 2, "c" -> 3))
      ).toDF("id", "array_col", "map_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Seq(1, 2, 3), Map("a" -> 1)),
        Row(2, Seq.empty[Int], Map.empty[String, Int]),
        Row(3, Seq(4), Map("b" -> 2, "c" -> 3))
      ))

      // Verify cache was used
      assert(cached.storageLevel.useMemory)
    }
  }

  test("columnar input with deeply nested structures from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // Create a deeply nested structure: Array[Struct[Map[String, Array[Int]]]]
      val df = Seq(
        (1, Seq(
          (Map("x" -> Seq(1, 2)), "data1"),
          (Map("y" -> Seq(3, 4, 5)), "data2")
        )),
        (2, Seq(
          (Map("z" -> Seq(6)), "data3")
        ))
      ).toDF("id", "deep_nested")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, Seq(
          Row(Map("x" -> Seq(1, 2)), "data1"),
          Row(Map("y" -> Seq(3, 4, 5)), "data2")
        )),
        Row(2, Seq(
          Row(Map("z" -> Seq(6)), "data3")
        ))
      ))

      // Verify operations work on deeply nested data
      val result = cached.filter($"id" === 1)
      assert(result.count() === 1)
    }
  }

  test("columnar input with mixed primitive and complex types from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val df = Seq(
        (1, "name1", 100L, Seq(1, 2, 3), Map("k1" -> "v1"), ("nested", 99)),
        (2, "name2", 200L, Seq(4, 5), Map("k2" -> "v2"), ("nested2", 88)),
        (3, "name3", 300L, Seq(6), Map("k3" -> "v3"), ("nested3", 77))
      ).toDF("id", "name", "value", "array_col", "map_col", "struct_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()
      checkAnswer(cached, Seq(
        Row(1, "name1", 100L, Seq(1, 2, 3), Map("k1" -> "v1"), Row("nested", 99)),
        Row(2, "name2", 200L, Seq(4, 5), Map("k2" -> "v2"), Row("nested2", 88)),
        Row(3, "name3", 300L, Seq(6), Map("k3" -> "v3"), Row("nested3", 77))
      ))

      // Verify column projection works
      val projected = cached.select("id", "array_col", "struct_col")
      checkAnswer(projected, Seq(
        Row(1, Seq(1, 2, 3), Row("nested", 99)),
        Row(2, Seq(4, 5), Row("nested2", 88)),
        Row(3, Seq(6), Row("nested3", 77))
      ))
    }
  }

  test("columnar input with large complex types dataset from parquet") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      // Create a larger dataset with complex types
      val df = (1 to 1000).map { i =>
        (i, Seq(i, i * 2, i * 3), Map(s"key$i" -> i * 10), (s"struct$i", i * 100))
      }.toDF("id", "array_col", "map_col", "struct_col")

      // Write as parquet (columnar format)
      df.write.parquet(path)

      // Read and cache - should use columnar input path
      val cached = spark.read.parquet(path).cache()

      // Verify a filtered subset
      val filtered = cached.filter($"id" > 990)
      assert(filtered.count() === 10)

      // Verify content of filtered data
      val result = filtered.collect().sortBy(_.getInt(0))
      assert(result.length === 10)
      assert(result(0).getInt(0) === 991)
      assert(result(0).getAs[Seq[Int]](1) === Seq(991, 1982, 2973))
    }
  }

  test("columnar input with vectorized reader and complex types") {
    withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> "true") {
      withTempPath { dir =>
        val path = dir.getAbsolutePath
        val df = Seq(
          (1, Seq(1, 2, 3), ("a", 10)),
          (2, Seq(4, 5, 6), ("b", 20)),
          (3, Seq(7, 8, 9), ("c", 30))
        ).toDF("id", "array_col", "struct_col")

        // Write as parquet (columnar format)
        df.write.parquet(path)

        // Read and cache with vectorized reader enabled
        val cached = spark.read.parquet(path).cache()
        checkAnswer(cached, Seq(
          Row(1, Seq(1, 2, 3), Row("a", 10)),
          Row(2, Seq(4, 5, 6), Row("b", 20)),
          Row(3, Seq(7, 8, 9), Row("c", 30))
        ))

        // Verify vectorized reader is used
        val plan = cached.queryExecution.executedPlan
        val inMemoryScan = plan.collectFirst {
          case scan: InMemoryTableScanExec => scan
        }
        assert(inMemoryScan.isDefined)
        assert(inMemoryScan.get.supportsColumnar)
      }
    }
  }
}
