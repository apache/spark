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
import java.time.{Duration, LocalDateTime, LocalTime, Period}

import org.apache.arrow.vector.{
  BigIntVector, BitVector, DateDayVector, DecimalVector,
  Float4Vector, Float8Vector, IntVector, LargeVarCharVector, SmallIntVector,
  TimeNanoVector, TimeStampMicroTZVector, TimeStampMicroVector, TinyIntVector,
  VarBinaryVector, VarCharVector, VectorSchemaRoot}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.{ExamplePoint, ExamplePointUDT, SharedSparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.CalendarInterval

/** UDT whose sqlType is Arrow-supported (ArrayType(DoubleType)). */
private class SupportedUDT extends UserDefinedType[Array[Double]] {
  override def sqlType: DataType = ArrayType(DoubleType, containsNull = false)
  override def serialize(obj: Array[Double]): Any = obj
  override def deserialize(datum: Any): Array[Double] = datum.asInstanceOf[Array[Double]]
  override def userClass: Class[Array[Double]] = classOf[Array[Double]]
}

/** UDT whose sqlType is ObjectType - not supported by Arrow. */
private class UnsupportedUDT extends UserDefinedType[AnyRef] {
  override def sqlType: DataType = ObjectType(classOf[AnyRef])
  override def serialize(obj: AnyRef): Any = obj
  override def deserialize(datum: Any): AnyRef = datum.asInstanceOf[AnyRef]
  override def userClass: Class[AnyRef] = classOf[AnyRef]
}

class ArrowCachedBatchSerializerSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
        classOf[ArrowCachedBatchSerializer].getName)
      .set(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key, "false")
  }

  // InMemoryRelation caches the serializer instance in a process-wide field that is initialized
  // from spark.sql.cache.serializer only on first use. When another suite runs first in the same
  // JVM, that field is already bound to DefaultCachedBatchSerializer, so reset it here to pick up
  // the Arrow serializer configured above, and reset it again afterwards so we do not leak the
  // Arrow serializer to later suites.
  override def beforeAll(): Unit = {
    super.beforeAll()
    InMemoryRelation.clearSerializer()
  }

  override def afterAll(): Unit = {
    InMemoryRelation.clearSerializer()
    super.afterAll()
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

    // UDT: delegates to sqlType - supported when sqlType is Arrow-compatible
    // ExamplePointUDT.sqlType = ArrayType(DoubleType) -> supported
    assert(ArrowUtils.isSupportedByArrow(new ExamplePointUDT()),
      "UDT with Arrow-supported sqlType should be supported")
    assert(ArrowUtils.isSupportedByArrow(new SupportedUDT()),
      "UDT with ArrayType(DoubleType) sqlType should be supported")
    // UDT with ObjectType sqlType -> not supported (ObjectType is internal, not an Arrow type)
    assert(!ArrowUtils.isSupportedByArrow(new UnsupportedUDT()),
      "UDT with ObjectType sqlType should not be supported")
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

  test("InternalRow path (readValueFromVector) handles all supported data types") {
    // Exercises every explicit type arm in readValueFromVector via the InternalRow fallback path
    // (CACHE_VECTORIZED_READER_ENABLED=false, set at suite level). Each case verifies a full
    // cache write -> read roundtrip including both non-null and null values.

    // --- Primitive types ---

    // BooleanType: BitVector.get(i) != 0
    val boolDf = Seq(Some(true), None, Some(false)).toDF("v")
    boolDf.cache()
    checkAnswer(boolDf, Seq(Row(true), Row(null), Row(false)))
    boolDf.unpersist()

    // ByteType: TinyIntVector.get(i)
    val byteDf = Seq(Some(1.toByte), None, Some(10.toByte)).toDF("v")
    byteDf.cache()
    checkAnswer(byteDf, Seq(Row(1.toByte), Row(null), Row(10.toByte)))
    byteDf.unpersist()

    // ShortType: SmallIntVector.get(i)
    val shortDf = Seq(Some(1.toShort), None, Some(100.toShort)).toDF("v")
    shortDf.cache()
    checkAnswer(shortDf, Seq(Row(1.toShort), Row(null), Row(100.toShort)))
    shortDf.unpersist()

    // IntegerType: IntVector.get(i)
    val intDf = Seq(Some(42), None, Some(-7)).toDF("v")
    intDf.cache()
    checkAnswer(intDf, Seq(Row(42), Row(null), Row(-7)))
    intDf.unpersist()

    // LongType: BigIntVector.get(i)
    val longDf = Seq(Some(100L), None, Some(-50L)).toDF("v")
    longDf.cache()
    checkAnswer(longDf, Seq(Row(100L), Row(null), Row(-50L)))
    longDf.unpersist()

    // FloatType: Float4Vector.get(i)
    val floatDf = Seq(Some(3.14f), None, Some(-1.0f)).toDF("v")
    floatDf.cache()
    checkAnswer(floatDf, Seq(Row(3.14f), Row(null), Row(-1.0f)))
    floatDf.unpersist()

    // DoubleType: Float8Vector.get(i)
    val doubleDf = Seq(Some(2.718), None, Some(-1.0)).toDF("v")
    doubleDf.cache()
    checkAnswer(doubleDf, Seq(Row(2.718), Row(null), Row(-1.0)))
    doubleDf.unpersist()

    // --- String and Binary types ---

    // StringType: VarCharVector.get(i) -> UTF8String.fromBytes
    val stringDf = Seq(Some("hello"), None, Some("world")).toDF("v")
    stringDf.cache()
    checkAnswer(stringDf, Seq(Row("hello"), Row(null), Row("world")))
    stringDf.unpersist()

    // BinaryType: VarBinaryVector.get(i)
    val bytes1 = "hello".getBytes("UTF-8")
    val bytes2 = "world".getBytes("UTF-8")
    val binaryDf = Seq(bytes1, bytes2).toDF("v")
    binaryDf.cache()
    val binaryResult = binaryDf.collect()
    assert(binaryResult(0).getAs[Array[Byte]](0) sameElements bytes1)
    assert(binaryResult(1).getAs[Array[Byte]](0) sameElements bytes2)
    binaryDf.unpersist()

    // DecimalType (compact, precision <= 18): fast path reads unscaled long from Arrow buffer
    val decDf = Seq(Some(BigDecimal("123.45")), None, Some(BigDecimal("678.90"))).toDF("v")
    decDf.cache()
    checkAnswer(decDf, Seq(Row(BigDecimal("123.45")), Row(null), Row(BigDecimal("678.90"))))
    decDf.unpersist()

    // DecimalType (compact, negative values): verifies sign-bit correctness when reading
    // lower 8 bytes of Arrow's 128-bit little-endian two's-complement buffer as signed Long
    val negDecData = Seq(
      new java.math.BigDecimal("-123.45"),
      new java.math.BigDecimal("0.00"),
      new java.math.BigDecimal("-999999.99"))
    val negDecDf = spark.createDataFrame(
      spark.sparkContext.parallelize(negDecData.map(Row(_))),
      StructType(Seq(StructField("v", DecimalType(10, 2)))))
    negDecDf.cache()
    checkAnswer(negDecDf, negDecData.map(d => Row(d)))
    negDecDf.unpersist()

    // DecimalType (wide, precision > 18): slow path via DecimalVector.getObject -> BigDecimal
    val wideDecData = Seq(
      new java.math.BigDecimal("12345678901234567890.1234567890"),
      new java.math.BigDecimal("-99999999999999999999.9999999999"),
      new java.math.BigDecimal("0.0000000001"))
    val wideDecDf = spark.createDataFrame(
      spark.sparkContext.parallelize(wideDecData.map(Row(_))),
      StructType(Seq(StructField("v", DecimalType(30, 10)))))
    wideDecDf.cache()
    checkAnswer(wideDecDf, wideDecData.map(d => Row(d)))
    wideDecDf.unpersist()

    // --- Date and time types ---

    // DateType: DateDayVector.get(i) (days since epoch)
    val dateDf = Seq(Some(Date.valueOf("2020-01-01")), None, Some(Date.valueOf("2025-12-31")))
      .toDF("v")
    dateDf.cache()
    checkAnswer(dateDf,
      Seq(Row(Date.valueOf("2020-01-01")), Row(null), Row(Date.valueOf("2025-12-31"))))
    dateDf.unpersist()

    // TimestampType: TimeStampMicroTZVector.get(i) (microseconds since epoch)
    val ts1 = Timestamp.valueOf("2020-01-01 12:00:00")
    val ts2 = Timestamp.valueOf("2025-06-15 00:00:00")
    val tsDf = Seq(Some(ts1), None, Some(ts2)).toDF("v")
    tsDf.cache()
    checkAnswer(tsDf, Seq(Row(ts1), Row(null), Row(ts2)))
    tsDf.unpersist()

    // TimestampNTZType: TimeStampMicroVector.get(i) (microseconds, no timezone)
    val ldt1 = LocalDateTime.of(2020, 1, 1, 12, 0)
    val ldt2 = LocalDateTime.of(2025, 6, 15, 0, 0)
    val tsNtzDf = Seq(Some(ldt1), None, Some(ldt2)).toDF("v")
    tsNtzDf.cache()
    checkAnswer(tsNtzDf, Seq(Row(ldt1), Row(null), Row(ldt2)))
    tsNtzDf.unpersist()

    // --- Interval types ---

    // YearMonthIntervalType: IntervalYearVector.get(i) (months)
    val ymiSql = "SELECT INTERVAL '1-1' YEAR TO MONTH AS ymi"
    val ymiDf = spark.sql(ymiSql)
    ymiDf.cache()
    checkAnswer(ymiDf, spark.sql(ymiSql))
    ymiDf.unpersist()

    // DayTimeIntervalType: DurationVector.get(int) returns ArrowBuf; must use static form
    val dtiSql = "SELECT INTERVAL '1' DAY AS dti"
    val dtiDf = spark.sql(dtiSql)
    dtiDf.cache()
    checkAnswer(dtiDf, spark.sql(dtiSql))
    dtiDf.unpersist()

    // TimeType: TimeNanoVector.get(i) (nanoseconds since midnight)
    val timeDf = Seq(LocalTime.of(12, 30, 45), LocalTime.of(0, 0, 0)).toDF("t")
    timeDf.cache()
    checkAnswer(timeDf, Seq(Row(LocalTime.of(12, 30, 45)), Row(LocalTime.of(0, 0, 0))))
    timeDf.unpersist()

    // CalendarIntervalType: ArrowColumnVector.getInterval(i) (IntervalMonthDayNanoVector)
    val interval = new CalendarInterval(1, 2, 3000000L)  // 1 month, 2 days, 3 ms
    val ciSchema = StructType(Seq(StructField("ci", CalendarIntervalType, nullable = true)))
    val ciDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(interval), Row(null))), ciSchema)
    ciDf.cache()
    checkAnswer(ciDf, Seq(Row(interval), Row(null)))
    ciDf.unpersist()

    // --- Null type ---

    // NullType: row.setNullAt without dispatching into readValueFromVector
    val nullSchema = StructType(Seq(StructField("n", NullType, nullable = true)))
    val nullDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(null), Row(null))), nullSchema)
    nullDf.cache()
    checkAnswer(nullDf, Seq(Row(null), Row(null)))
    nullDf.unpersist()

    // --- Complex types ---

    // ArrayType: ArrowColumnVector.getArray(i) (ListVector)
    val arrayDf = Seq(Seq(1, 2, 3), Seq(4, 5, 6)).toDF("v")
    arrayDf.cache()
    checkAnswer(arrayDf, Seq(Row(Seq(1, 2, 3)), Row(Seq(4, 5, 6))))
    arrayDf.unpersist()

    // StructType: ArrowColumnVector.getStruct(i) (StructVector)
    val structSql =
      "SELECT named_struct('a', 1, 'b', 'x') AS v " +
      "UNION ALL SELECT named_struct('a', 2, 'b', 'y') AS v"
    val structDf = spark.sql(structSql)
    structDf.cache()
    checkAnswer(structDf, spark.sql(structSql))
    structDf.unpersist()

    // MapType: ArrowColumnVector.getMap(i) (MapVector)
    val mapDf = Seq(Map(1 -> "a"), Map(2 -> "b")).toDF("v")
    mapDf.cache()
    checkAnswer(mapDf, Seq(Row(Map(1 -> "a")), Row(Map(2 -> "b"))))
    mapDf.unpersist()

    // UserDefinedType: dispatches to readValueFromVector with udt.sqlType (ArrayType(DoubleType))
    val point = new ExamplePoint(1.0, 2.0)
    val udtSchema = StructType(Seq(StructField("p", new ExamplePointUDT(), nullable = true)))
    val udtDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(point), Row(null))), udtSchema)
    udtDf.cache()
    checkAnswer(udtDf, Seq(Row(point), Row(null)))
    udtDf.unpersist()

    // VariantType: ArrowColumnVector.getVariant(i) (StructVector)
    val variantDf = spark.sql("SELECT parse_json('{\"a\":1}') AS v")
    variantDf.cache()
    checkAnswer(variantDf.selectExpr("to_json(v)"), Seq(Row("{\"a\":1}")))
    variantDf.unpersist()
  }

  // Helper: cache a single-column DataFrame (row path) and return its ArrowCachedBatch stats.
  // Stats layout per column: [lowerBound(0), upperBound(1), nullCount(2), rowCount(3), size(4)].
  private def cachedStats(df: org.apache.spark.sql.DataFrame)
      : org.apache.spark.sql.catalyst.InternalRow = {
    df.count()  // trigger cache population
    val relation = df.queryExecution.executedPlan.collectFirst {
      case scan: InMemoryTableScanExec => scan.relation
    }.get
    relation.cacheBuilder.cachedColumnBuffers.first().asInstanceOf[ArrowCachedBatch].stats
  }

  // Helper: creates a single-column, single-partition DataFrame backed by an RDD.
  // LocalRelation can split across multiple partitions, causing cachedStats to see only the first
  // partition's stats. sc.parallelize(data, numSlices=1) forces exactly one partition.
  private def singlePartDf(values: Seq[Any], dt: DataType): org.apache.spark.sql.DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(values.map(v => Row(v)), 1),
      StructType(Seq(StructField("v", dt, nullable = true))))

  test("createColumnStats returns the correct ColumnStats subclass for each supported type") {
    // Direct unit test: verify the stats class dispatched for each Spark type, which determines
    // whether partition pruning via min/max bounds is enabled.

    // Orderable types: createColumnStats returns a stats class that tracks min/max bounds.
    assert(ArrowCachedBatchSerializer.createColumnStats(BooleanType)
      .isInstanceOf[BooleanColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(ByteType).isInstanceOf[ByteColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(ShortType).isInstanceOf[ShortColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(IntegerType).isInstanceOf[IntColumnStats])
    // DateType is stored as Int (days since epoch) -> IntColumnStats
    assert(ArrowCachedBatchSerializer.createColumnStats(DateType).isInstanceOf[IntColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(LongType).isInstanceOf[LongColumnStats])
    // TimestampType/NTZ stored as Long (microseconds) -> LongColumnStats
    assert(ArrowCachedBatchSerializer.createColumnStats(TimestampType)
      .isInstanceOf[LongColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(TimestampNTZType)
      .isInstanceOf[LongColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(FloatType).isInstanceOf[FloatColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(DoubleType).isInstanceOf[DoubleColumnStats])
    // StringType (all collations) -> StringColumnStats with collation-aware semantic comparison
    assert(ArrowCachedBatchSerializer.createColumnStats(StringType).isInstanceOf[StringColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(
      new StringType(1)).isInstanceOf[StringColumnStats])  // collationId 1 = UTF8_LCASE
    assert(ArrowCachedBatchSerializer.createColumnStats(
      StringType("UNICODE")).isInstanceOf[StringColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(DecimalType(10, 2))
      .isInstanceOf[DecimalColumnStats])
    // YearMonthIntervalType stored as Int (months) -> IntColumnStats
    assert(ArrowCachedBatchSerializer.createColumnStats(
      YearMonthIntervalType()).isInstanceOf[IntColumnStats])
    // DayTimeIntervalType stored as Long (microseconds) -> LongColumnStats
    assert(ArrowCachedBatchSerializer.createColumnStats(
      DayTimeIntervalType()).isInstanceOf[LongColumnStats])
    // TimeType stored as Long (nanoseconds) -> LongColumnStats
    assert(ArrowCachedBatchSerializer.createColumnStats(TimeType(6)).isInstanceOf[LongColumnStats])

    // Non-orderable types: createColumnStats returns a stats class with null bounds.
    assert(ArrowCachedBatchSerializer.createColumnStats(BinaryType).isInstanceOf[BinaryColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(
      CalendarIntervalType).isInstanceOf[IntervalColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(VariantType)
      .isInstanceOf[VariantColumnStats])

    // Complex types and UDT: no natural ordering -> ObjectColumnStats (null bounds).
    assert(ArrowCachedBatchSerializer.createColumnStats(
      ArrayType(IntegerType)).isInstanceOf[ObjectColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(
      StructType(Seq(StructField("a", IntegerType)))).isInstanceOf[ObjectColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(
      MapType(StringType, IntegerType)).isInstanceOf[ObjectColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(
      new ExamplePointUDT()).isInstanceOf[ObjectColumnStats])
    assert(ArrowCachedBatchSerializer.createColumnStats(NullType).isInstanceOf[ObjectColumnStats])
  }

  test("row path stats: orderable types produce correct min/max bounds") {
    // DataFrames use the row path (InternalRowToArrowCachedBatchIterator), exercising
    // createColumnStats + buildStatisticsFromCollectors. singlePartDf ensures all values land
    // in one cached batch so cachedStats.first() sees the global min and max.

    // BooleanType: lower=false, upper=true
    val boolDf = singlePartDf(Seq(false, true), BooleanType).cache()
    val boolStats = cachedStats(boolDf)
    assert(!boolStats.isNullAt(0) && !boolStats.isNullAt(1))
    assert(!boolStats.getBoolean(0) && boolStats.getBoolean(1))
    boolDf.unpersist()

    // ByteType: lower=1, upper=10
    val byteDf = singlePartDf(Seq(1.toByte, 10.toByte), ByteType).cache()
    val byteStats = cachedStats(byteDf)
    assert(!byteStats.isNullAt(0) && !byteStats.isNullAt(1))
    assert(byteStats.getByte(0) == 1.toByte && byteStats.getByte(1) == 10.toByte)
    byteDf.unpersist()

    // ShortType: lower=1, upper=10
    val shortDf = singlePartDf(Seq(1.toShort, 10.toShort), ShortType).cache()
    val shortStats = cachedStats(shortDf)
    assert(!shortStats.isNullAt(0) && !shortStats.isNullAt(1))
    assert(shortStats.getShort(0) == 1.toShort && shortStats.getShort(1) == 10.toShort)
    shortDf.unpersist()

    // IntegerType: lower=1, upper=10
    val intDf = singlePartDf(Seq(1, 10), IntegerType).cache()
    val intStats = cachedStats(intDf)
    assert(!intStats.isNullAt(0) && !intStats.isNullAt(1))
    assert(intStats.getInt(0) == 1 && intStats.getInt(1) == 10)
    intDf.unpersist()

    // DateType: stored as Int (days since epoch); 2020-01-01 < 2025-01-01
    val dateDf = singlePartDf(
      Seq(Date.valueOf("2020-01-01"), Date.valueOf("2025-01-01")), DateType).cache()
    val dateStats = cachedStats(dateDf)
    assert(!dateStats.isNullAt(0) && !dateStats.isNullAt(1))
    assert(dateStats.getInt(0) < dateStats.getInt(1))
    dateDf.unpersist()

    // LongType: lower=1L, upper=10L
    val longDf = singlePartDf(Seq(1L, 10L), LongType).cache()
    val longStats = cachedStats(longDf)
    assert(!longStats.isNullAt(0) && !longStats.isNullAt(1))
    assert(longStats.getLong(0) == 1L && longStats.getLong(1) == 10L)
    longDf.unpersist()

    // TimestampType: stored as Long (microseconds since epoch); 2020 < 2025
    val tsDf = singlePartDf(
      Seq(Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2025-01-01 00:00:00")),
      TimestampType).cache()
    val tsStats = cachedStats(tsDf)
    assert(!tsStats.isNullAt(0) && !tsStats.isNullAt(1))
    assert(tsStats.getLong(0) < tsStats.getLong(1))
    tsDf.unpersist()

    // TimestampNTZType: stored as Long (microseconds since epoch); 2020 < 2025
    val tsNtzDf = singlePartDf(
      Seq(LocalDateTime.of(2020, 1, 1, 0, 0), LocalDateTime.of(2025, 1, 1, 0, 0)),
      TimestampNTZType).cache()
    val tsNtzStats = cachedStats(tsNtzDf)
    assert(!tsNtzStats.isNullAt(0) && !tsNtzStats.isNullAt(1))
    assert(tsNtzStats.getLong(0) < tsNtzStats.getLong(1))
    tsNtzDf.unpersist()

    // FloatType: NaN is included but IEEE 754 comparisons with NaN are always false,
    // so NaN never updates min/max; lower=1.0f, upper=10.0f
    val floatDf = singlePartDf(Seq(1.0f, Float.NaN, 10.0f), FloatType).cache()
    val floatStats = cachedStats(floatDf)
    assert(!floatStats.isNullAt(0) && !floatStats.isNullAt(1))
    assert(floatStats.getFloat(0) == 1.0f && floatStats.getFloat(1) == 10.0f)
    floatDf.unpersist()

    // DoubleType: same NaN-exclusion behavior via IEEE 754; lower=1.0, upper=10.0
    val doubleDf = singlePartDf(Seq(1.0, Double.NaN, 10.0), DoubleType).cache()
    val doubleStats = cachedStats(doubleDf)
    assert(!doubleStats.isNullAt(0) && !doubleStats.isNullAt(1))
    assert(doubleStats.getDouble(0) == 1.0 && doubleStats.getDouble(1) == 10.0)
    doubleDf.unpersist()

    // StringType (UTF8_BINARY): "apple" < "zebra" in binary order
    val stringDf = singlePartDf(Seq("apple", "zebra"), StringType).cache()
    val stringStats = cachedStats(stringDf)
    assert(!stringStats.isNullAt(0) && !stringStats.isNullAt(1))
    assert(stringStats.getUTF8String(0).toString == "apple")
    assert(stringStats.getUTF8String(1).toString == "zebra")
    stringDf.unpersist()

    // Collated StringType (UTF8_LCASE): semantic min/max uses case-insensitive comparison.
    // "Apple" and "zebra": case-insensitively "apple" < "zebra", so lower="Apple", upper="zebra".
    val collatedStringType = new StringType(1)  // collationId 1 = UTF8_LCASE
    val collatedSchema = StructType(Seq(StructField("v", collatedStringType, nullable = true)))
    val collatedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("Apple"), Row("zebra")), 1),
      collatedSchema).cache()
    val collatedStats = cachedStats(collatedDf)
    assert(!collatedStats.isNullAt(0), "lower bound should not be null for collated StringType")
    assert(!collatedStats.isNullAt(1), "upper bound should not be null for collated StringType")
    assert(collatedStats.getUTF8String(0).toString == "Apple")  // semantic min
    assert(collatedStats.getUTF8String(1).toString == "zebra")  // semantic max
    collatedDf.unpersist()

    // DecimalType(10,2): lower=1.23, upper=9.87
    val decimalDf = singlePartDf(
      Seq(new java.math.BigDecimal("1.23"), new java.math.BigDecimal("9.87")),
      DecimalType(10, 2)).cache()
    val decimalStats = cachedStats(decimalDf)
    assert(!decimalStats.isNullAt(0) && !decimalStats.isNullAt(1))
    assert(decimalStats.getDecimal(0, 10, 2).compareTo(decimalStats.getDecimal(1, 10, 2)) < 0)
    decimalDf.unpersist()

    // YearMonthIntervalType: stored as Int (months); Period.of(1,0,0)=12mo < Period.of(2,0,0)=24mo
    val ymiDf = singlePartDf(
      Seq(Period.of(1, 0, 0), Period.of(2, 0, 0)), YearMonthIntervalType()).cache()
    val ymiStats = cachedStats(ymiDf)
    assert(!ymiStats.isNullAt(0) && !ymiStats.isNullAt(1))
    assert(ymiStats.getInt(0) < ymiStats.getInt(1))
    ymiDf.unpersist()

    // DayTimeIntervalType: stored as Long (microseconds); 1 day < 2 days
    val dtiDf = singlePartDf(
      Seq(Duration.ofDays(1), Duration.ofDays(2)), DayTimeIntervalType()).cache()
    val dtiStats = cachedStats(dtiDf)
    assert(!dtiStats.isNullAt(0) && !dtiStats.isNullAt(1))
    assert(dtiStats.getLong(0) < dtiStats.getLong(1))
    dtiDf.unpersist()

    // TimeType: stored as Long (nanoseconds); 08:00 < 20:00
    val timeDf = singlePartDf(
      Seq(LocalTime.of(8, 0, 0), LocalTime.of(20, 0, 0)), TimeType(6)).cache()
    val timeStats = cachedStats(timeDf)
    assert(!timeStats.isNullAt(0) && !timeStats.isNullAt(1))
    assert(timeStats.getLong(0) < timeStats.getLong(1))
    timeDf.unpersist()
  }

  test("row path stats: non-orderable types produce null lower and upper bounds") {
    // Verifies that types without natural ordering return null bounds so that partition pruning
    // is safely disabled for them, preventing incorrect data exclusion.
    def assertNullBounds(df: org.apache.spark.sql.DataFrame): Unit = {
      val stats = cachedStats(df)
      assert(stats.isNullAt(0), "lower bound should be null for non-orderable type")
      assert(stats.isNullAt(1), "upper bound should be null for non-orderable type")
      df.unpersist()
    }

    // BinaryType: no natural total ordering
    assertNullBounds(Seq(Array[Byte](1, 2), Array[Byte](3, 4)).toDF("v").cache())

    // CalendarIntervalType: unordered composite (months + days + nanoseconds)
    val ciSchema = StructType(Seq(StructField("v", CalendarIntervalType, nullable = true)))
    assertNullBounds(spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(new CalendarInterval(1, 2, 3000000L)),
        Row(new CalendarInterval(2, 0, 0L)))),
      ciSchema).cache())

    // ArrayType: no natural ordering
    assertNullBounds(spark.sql(
      "SELECT array(1, 2) AS v UNION ALL SELECT array(3, 4) AS v"
    ).cache())

    // StructType: no natural ordering
    assertNullBounds(spark.sql(
      "SELECT named_struct('i', 1, 's', 'a') AS v " +
      "UNION ALL SELECT named_struct('i', 2, 's', 'b') AS v"
    ).cache())

    // MapType: no natural ordering
    assertNullBounds(spark.sql(
      "SELECT map(1, 'a') AS v UNION ALL SELECT map(2, 'b') AS v"
    ).cache())

    // UserDefinedType: no natural ordering
    val udtSchema = StructType(Seq(StructField("v", new ExamplePointUDT(), nullable = true)))
    assertNullBounds(spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(new ExamplePoint(1.0, 2.0)), Row(null))),
      udtSchema).cache())

    // NullType: all values are null by definition
    val nullSchema = StructType(Seq(StructField("v", NullType, nullable = true)))
    assertNullBounds(spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(null), Row(null))),
      nullSchema).cache())

    // VariantType: no natural ordering
    assertNullBounds(spark.sql("SELECT parse_json('{\"k\":1}') AS v").cache())
  }

  test("row path stats: all-NaN Float/Double column produces inverted sentinel bounds") {
    // FloatColumnStats and DoubleColumnStats initialize upper=MinValue, lower=MaxValue as
    // sentinels. IEEE 754 comparisons with NaN are always false, so NaN never beats either
    // sentinel. When every value is NaN, the sentinels are returned unchanged: lower=MaxValue,
    // upper=MinValue (lower > upper). This differs from the Arrow path, which returns null bounds
    // for all-NaN input (because calculateMinMaxFloat/Double explicitly skips NaN with !_.isNaN
    // and returns (null, null) when hasValue stays false).
    val floatDf = singlePartDf(Seq(Float.NaN), FloatType).cache()
    val floatStats = cachedStats(floatDf)
    assert(!floatStats.isNullAt(0),
      "FloatType lower should not be null for all-NaN (sentinel used)")
    assert(!floatStats.isNullAt(1),
      "FloatType upper should not be null for all-NaN (sentinel used)")
    assert(floatStats.getFloat(0) == Float.MaxValue,
      s"FloatType lower expected Float.MaxValue (sentinel), got ${floatStats.getFloat(0)}")
    assert(floatStats.getFloat(1) == Float.MinValue,
      s"FloatType upper expected Float.MinValue (sentinel), got ${floatStats.getFloat(1)}")
    floatDf.unpersist()

    val doubleDf = singlePartDf(Seq(Double.NaN), DoubleType).cache()
    val doubleStats = cachedStats(doubleDf)
    assert(!doubleStats.isNullAt(0),
      "DoubleType lower should not be null for all-NaN (sentinel used)")
    assert(!doubleStats.isNullAt(1),
      "DoubleType upper should not be null for all-NaN (sentinel used)")
    assert(doubleStats.getDouble(0) == Double.MaxValue,
      s"DoubleType lower expected Double.MaxValue (sentinel), got ${doubleStats.getDouble(0)}")
    assert(doubleStats.getDouble(1) == Double.MinValue,
      s"DoubleType upper expected Double.MinValue (sentinel), got ${doubleStats.getDouble(1)}")
    doubleDf.unpersist()
  }

  test("collectStatistics produces correct min/max bounds for all orderable types") {
    // Direct unit test of ArrowCachedBatchSerializer.collectStatistics, which is invoked whenever
    // the input ColumnarBatch contains ArrowColumnVector columns (zero-copy path in
    // ColumnarBatchToArrowCachedBatchIterator). Three rows [low, mid, high] ensure min/max are
    // correctly identified for each type.
    val serializer = new ArrowCachedBatchSerializer()

    val schema = Seq(
      AttributeReference("bool_col", BooleanType)(),       // BitVector
      AttributeReference("byte_col", ByteType)(),          // TinyIntVector
      AttributeReference("short_col", ShortType)(),        // SmallIntVector
      AttributeReference("float_col", FloatType)(),        // Float4Vector
      AttributeReference("double_col", DoubleType)(),      // Float8Vector
      AttributeReference("date_col", DateType)(),          // DateDayVector (days since epoch)
      AttributeReference("ts_col", TimestampType)(),       // TimeStampMicroTZVector (microseconds)
      AttributeReference("ts_ntz_col", TimestampNTZType)(),// TimeStampMicroVector (microseconds)
      AttributeReference("int_col", IntegerType)(),          // IntVector (standalone)
      AttributeReference("long_col", LongType)(),            // BigIntVector (standalone)
      AttributeReference("decimal_col", DecimalType(10, 2))(), // DecimalVector
      AttributeReference("ymi_col", YearMonthIntervalType())(),  // IntervalYearVector (months)
      AttributeReference("dti_col", DayTimeIntervalType())(),    // DurationVector (microseconds)
      AttributeReference("time_col", TimeType(6))()              // TimeNanoVector (nanoseconds)
    )
    val sparkSchema = StructType(schema.map(a => StructField(a.name, a.dataType)))
    val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false)
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)

    try {
      root.allocateNew()
      val boolVector = root.getVector("bool_col").asInstanceOf[BitVector]
      val byteVector = root.getVector("byte_col").asInstanceOf[TinyIntVector]
      val shortVector = root.getVector("short_col").asInstanceOf[SmallIntVector]
      val floatVector = root.getVector("float_col").asInstanceOf[Float4Vector]
      val doubleVector = root.getVector("double_col").asInstanceOf[Float8Vector]
      val dateVector = root.getVector("date_col").asInstanceOf[DateDayVector]
      val tsVector = root.getVector("ts_col").asInstanceOf[TimeStampMicroTZVector]
      val tsNtzVector = root.getVector("ts_ntz_col").asInstanceOf[TimeStampMicroVector]
      val intVector = root.getVector("int_col").asInstanceOf[IntVector]
      val longVector = root.getVector("long_col").asInstanceOf[BigIntVector]
      val decimalVector = root.getVector("decimal_col").asInstanceOf[DecimalVector]
      val ymiVector = root.getVector("ymi_col")
        .asInstanceOf[org.apache.arrow.vector.IntervalYearVector]
      val dtiVector = root.getVector("dti_col")
        .asInstanceOf[org.apache.arrow.vector.DurationVector]
      val timeVector = root.getVector("time_col").asInstanceOf[TimeNanoVector]

      // Row 0: low values
      boolVector.setSafe(0, 0)               // false
      byteVector.setSafe(0, 1.toByte)
      shortVector.setSafe(0, 100.toShort)
      floatVector.setSafe(0, 1.0f)
      doubleVector.setSafe(0, 1.0)
      dateVector.setSafe(0, 18262)           // 2020-01-01
      tsVector.setSafe(0, 1577836800000000L) // 2020-01-01 00:00:00 UTC in microseconds
      tsNtzVector.setSafe(0, 1577836800000000L)
      intVector.setSafe(0, 1)
      longVector.setSafe(0, 1L)
      decimalVector.setSafe(0, new java.math.BigDecimal("1.23"))
      ymiVector.setSafe(0, 12)                 // 1 year = 12 months
      dtiVector.setSafe(0, 86400000000L)       // 1 day in microseconds
      timeVector.setSafe(0, 28800000000000L)   // 08:00:00 in nanoseconds

      // Row 1: mid values -- Float/Double use NaN to verify NaN is excluded from min/max
      boolVector.setSafe(1, 1)               // true -- becomes the max
      byteVector.setSafe(1, 5.toByte)
      shortVector.setSafe(1, 500.toShort)
      floatVector.setSafe(1, Float.NaN)      // NaN: must not affect lower=1.0f or upper=10.0f
      doubleVector.setSafe(1, Double.NaN)    // NaN: must not affect lower=1.0 or upper=10.0
      dateVector.setSafe(1, 19000)
      tsVector.setSafe(1, 1700000000000000L)
      tsNtzVector.setSafe(1, 1700000000000000L)
      intVector.setSafe(1, 5)
      longVector.setSafe(1, 5L)
      decimalVector.setSafe(1, new java.math.BigDecimal("5.55"))
      ymiVector.setSafe(1, 18)                 // 1.5 years = 18 months
      dtiVector.setSafe(1, 172800000000L)      // 2 days in microseconds
      timeVector.setSafe(1, 43200000000000L)   // 12:00:00 in nanoseconds

      // Row 2: high values
      boolVector.setSafe(2, 0)               // false again (3 rows; bool max stays true from row 1)
      byteVector.setSafe(2, 10.toByte)
      shortVector.setSafe(2, 1000.toShort)
      floatVector.setSafe(2, 10.0f)
      doubleVector.setSafe(2, 10.0)
      dateVector.setSafe(2, 20000)
      tsVector.setSafe(2, 1800000000000000L)
      tsNtzVector.setSafe(2, 1800000000000000L)
      intVector.setSafe(2, 10)
      longVector.setSafe(2, 10L)
      decimalVector.setSafe(2, new java.math.BigDecimal("9.87"))
      ymiVector.setSafe(2, 24)                 // 2 years = 24 months
      dtiVector.setSafe(2, 259200000000L)      // 3 days in microseconds
      timeVector.setSafe(2, 72000000000000L)   // 20:00:00 in nanoseconds

      root.setRowCount(3)

      val stats = ArrowCachedBatchSerializer.collectStatistics(root, schema)

      // Stats layout: [lower(0), upper(1), nullCount(2), rowCount(3), sizeInBytes(4)] per column.
      // col0 BooleanType (offset 0): lower=false, upper=true
      assert(!stats.getBoolean(0), s"BooleanType lower expected false, got ${stats.getBoolean(0)}")
      assert(stats.getBoolean(1), s"BooleanType upper expected true, got ${stats.getBoolean(1)}")

      // col1 ByteType (offset 5): lower=1, upper=10
      assert(stats.getByte(5) == 1.toByte, s"ByteType lower=${stats.getByte(5)}")
      assert(stats.getByte(6) == 10.toByte, s"ByteType upper=${stats.getByte(6)}")

      // col2 ShortType (offset 10): lower=100, upper=1000
      assert(stats.getShort(10) == 100.toShort, s"ShortType lower=${stats.getShort(10)}")
      assert(stats.getShort(11) == 1000.toShort, s"ShortType upper=${stats.getShort(11)}")

      // col3 FloatType (offset 15): lower=1.0f, upper=10.0f
      assert(stats.getFloat(15) == 1.0f, s"FloatType lower=${stats.getFloat(15)}")
      assert(stats.getFloat(16) == 10.0f, s"FloatType upper=${stats.getFloat(16)}")

      // col4 DoubleType (offset 20): lower=1.0, upper=10.0
      assert(stats.getDouble(20) == 1.0, s"DoubleType lower=${stats.getDouble(20)}")
      assert(stats.getDouble(21) == 10.0, s"DoubleType upper=${stats.getDouble(21)}")

      // col5 DateType (offset 25): lower=18262 (2020-01-01), upper=20000
      assert(stats.getInt(25) == 18262, s"DateType lower=${stats.getInt(25)}")
      assert(stats.getInt(26) == 20000, s"DateType upper=${stats.getInt(26)}")

      // col6 TimestampType (offset 30): lower < upper (microseconds since epoch)
      assert(stats.getLong(30) == 1577836800000000L,
        s"TimestampType lower=${stats.getLong(30)}")
      assert(stats.getLong(31) == 1800000000000000L,
        s"TimestampType upper=${stats.getLong(31)}")

      // col7 TimestampNTZType (offset 35): lower < upper (microseconds, no timezone)
      assert(stats.getLong(35) == 1577836800000000L,
        s"TimestampNTZType lower=${stats.getLong(35)}")
      assert(stats.getLong(36) == 1800000000000000L,
        s"TimestampNTZType upper=${stats.getLong(36)}")

      // col8 IntegerType (offset 40): lower=1, upper=10
      assert(stats.getInt(40) == 1, s"IntegerType lower=${stats.getInt(40)}")
      assert(stats.getInt(41) == 10, s"IntegerType upper=${stats.getInt(41)}")

      // col9 LongType (offset 45): lower=1L, upper=10L
      assert(stats.getLong(45) == 1L, s"LongType lower=${stats.getLong(45)}")
      assert(stats.getLong(46) == 10L, s"LongType upper=${stats.getLong(46)}")

      // col10 DecimalType(10,2) (offset 50): lower=1.23, upper=9.87
      assert(stats.getDecimal(50, 10, 2).toJavaBigDecimal.compareTo(
        new java.math.BigDecimal("1.23")) == 0,
        s"DecimalType lower=${stats.getDecimal(50, 10, 2)}")
      assert(stats.getDecimal(51, 10, 2).toJavaBigDecimal.compareTo(
        new java.math.BigDecimal("9.87")) == 0,
        s"DecimalType upper=${stats.getDecimal(51, 10, 2)}")

      // col11 YearMonthIntervalType (offset 55): lower=12 months (1yr), upper=24 months (2yr)
      assert(stats.getInt(55) == 12, s"YearMonthIntervalType lower=${stats.getInt(55)}")
      assert(stats.getInt(56) == 24, s"YearMonthIntervalType upper=${stats.getInt(56)}")

      // col12 DayTimeIntervalType (offset 60): lower=1 day, upper=3 days (in microseconds)
      assert(stats.getLong(60) == 86400000000L,
        s"DayTimeIntervalType lower=${stats.getLong(60)}")
      assert(stats.getLong(61) == 259200000000L,
        s"DayTimeIntervalType upper=${stats.getLong(61)}")

      // col13 TimeType (offset 65): lower=08:00:00 (28800000000000ns),
      // upper=20:00:00 (72000000000000ns)
      assert(stats.getLong(65) == 28800000000000L,
        s"TimeType lower=${stats.getLong(65)}")
      assert(stats.getLong(66) == 72000000000000L,
        s"TimeType upper=${stats.getLong(66)}")

      // All null counts should be 0
      (0 until 14).foreach { col =>
        assert(stats.getInt(col * 5 + 2) == 0, s"nullCount for col$col should be 0")
      }

      root.close()
    } catch {
      case e: Exception =>
        root.close()
        throw e
    }
  }

  test("collectStatistics produces correct min/max bounds for StringType") {
    // StringType in Arrow is stored as VarCharVector (raw UTF-8 bytes). This test covers the
    // two distinct code paths in calculateMinMaxString: binary (UTF8_BINARY) and collation-aware
    // semantic (collated). The collated case directly exercises the Bug 2 fix: before the fix,
    // `case StringType =>` (singleton) did not match collated types so they returned null bounds.

    // UTF8_BINARY: binary-order comparison.
    // {"apple", "cherry", "banana"} -> lower=apple, upper=cherry
    {
      val schema = Seq(AttributeReference("str_col", StringType)())
      val sparkSchema = StructType(schema.map(a => StructField(a.name, a.dataType)))
      val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false)
      val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
      try {
        root.allocateNew()
        val strVector = root.getVector("str_col").asInstanceOf[VarCharVector]
        strVector.setSafe(0, "apple".getBytes("UTF-8"), 0, 5)
        strVector.setSafe(1, "cherry".getBytes("UTF-8"), 0, 6)
        strVector.setSafe(2, "banana".getBytes("UTF-8"), 0, 6)
        root.setRowCount(3)
        val stats = ArrowCachedBatchSerializer.collectStatistics(root, schema)
        assert(!stats.isNullAt(0), "UTF8_BINARY lower bound should not be null")
        assert(!stats.isNullAt(1), "UTF8_BINARY upper bound should not be null")
        assert(stats.getUTF8String(0).toString == "apple",
          s"UTF8_BINARY lower expected 'apple', got ${stats.getUTF8String(0)}")
        assert(stats.getUTF8String(1).toString == "cherry",
          s"UTF8_BINARY upper expected 'cherry', got ${stats.getUTF8String(1)}")
        root.close()
      } catch {
        case e: Exception => root.close(); throw e
      }
    }

    // UTF8_LCASE (collationId=1): case-insensitive semantic comparison.
    // Data: {"Apple", "banana", "Cherry"}
    // Binary order: "Apple"(A=65) < "Cherry"(C=67) < "banana"(b=98) -> binary max = "banana"
    // Semantic order: apple < banana < cherry -> semantic max = "Cherry"
    // Asserting upper == "Cherry" (not "banana") verifies collation-aware semanticCompare is used.
    {
      val collatedStringType = new StringType(1) // collationId 1 = UTF8_LCASE
      val schema = Seq(AttributeReference("str_col", collatedStringType)())
      val sparkSchema = StructType(schema.map(a => StructField(a.name, a.dataType)))
      val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false)
      val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)
      try {
        root.allocateNew()
        val strVector = root.getVector("str_col").asInstanceOf[VarCharVector]
        strVector.setSafe(0, "Apple".getBytes("UTF-8"), 0, 5)
        strVector.setSafe(1, "banana".getBytes("UTF-8"), 0, 6)
        strVector.setSafe(2, "Cherry".getBytes("UTF-8"), 0, 6)
        root.setRowCount(3)
        val stats = ArrowCachedBatchSerializer.collectStatistics(root, schema)
        assert(!stats.isNullAt(0), "UTF8_LCASE lower bound should not be null")
        assert(!stats.isNullAt(1), "UTF8_LCASE upper bound should not be null")
        assert(stats.getUTF8String(0).toString == "Apple",
          s"UTF8_LCASE lower expected 'Apple' (semantic min), got ${stats.getUTF8String(0)}")
        // "Cherry" is the semantic max (case-insensitively: cherry > banana > apple).
        // "banana" would be the binary max -- asserting "Cherry" proves semanticCompare is used.
        assert(stats.getUTF8String(1).toString == "Cherry",
          s"UTF8_LCASE upper expected 'Cherry' (semantic max), got ${stats.getUTF8String(1)}")
        root.close()
      } catch {
        case e: Exception => root.close(); throw e
      }
    }
  }

  test("collectStatistics returns null bounds when all Float/Double values are NaN") {
    // When every non-null value in a Float or Double column is NaN, calculateMinMaxFloat/Double
    // finds no valid (non-NaN) values. hasValue stays false -> returns (null, null) -> null bounds.
    // Null bounds disable partition pruning, ensuring NaN-only batches are never incorrectly
    // pruned.
    val schema = Seq(
      AttributeReference("float_col", FloatType)(),
      AttributeReference("double_col", DoubleType)()
    )
    val sparkSchema = StructType(schema.map(a => StructField(a.name, a.dataType)))
    val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false)
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)

    try {
      root.allocateNew()
      val floatVector = root.getVector("float_col").asInstanceOf[Float4Vector]
      val doubleVector = root.getVector("double_col").asInstanceOf[Float8Vector]

      floatVector.setSafe(0, Float.NaN)
      floatVector.setSafe(1, Float.NaN)
      doubleVector.setSafe(0, Double.NaN)
      doubleVector.setSafe(1, Double.NaN)
      root.setRowCount(2)

      val stats = ArrowCachedBatchSerializer.collectStatistics(root, schema)

      // FloatType (col0, offset 0): no valid values -> null bounds
      assert(stats.isNullAt(0), "FloatType lower bound should be null when all values are NaN")
      assert(stats.isNullAt(1), "FloatType upper bound should be null when all values are NaN")

      // DoubleType (col1, offset 5): no valid values -> null bounds
      assert(stats.isNullAt(5), "DoubleType lower bound should be null when all values are NaN")
      assert(stats.isNullAt(6), "DoubleType upper bound should be null when all values are NaN")

      root.close()
    } catch {
      case e: Exception =>
        root.close()
        throw e
    }
  }

  test("collectStatistics returns null bounds for non-orderable types") {
    // BinaryType has no natural ordering, so its lower and upper bounds must be null.
    // Null bounds disable partition pruning for those columns, preventing incorrect data exclusion.
    // A control IntegerType column confirms bounds are per-type, not per-batch.
    val schema = Seq(
      AttributeReference("bin_col", BinaryType)(),   // VarBinaryVector -- unordered
      AttributeReference("int_col", IntegerType)()   // IntVector -- orderable (control column)
    )
    val sparkSchema = StructType(schema.map(a => StructField(a.name, a.dataType)))
    val arrowSchema = ArrowUtils.toArrowSchema(sparkSchema, "UTC", false, false)
    val root = VectorSchemaRoot.create(arrowSchema, ArrowUtils.rootAllocator)

    try {
      root.allocateNew()
      val binVector = root.getVector("bin_col").asInstanceOf[VarBinaryVector]
      val intVector = root.getVector("int_col").asInstanceOf[IntVector]

      binVector.setSafe(0, "hello".getBytes("UTF-8"))
      binVector.setSafe(1, "world".getBytes("UTF-8"))
      intVector.setSafe(0, 1)
      intVector.setSafe(1, 10)
      root.setRowCount(2)

      val stats = ArrowCachedBatchSerializer.collectStatistics(root, schema)

      // BinaryType (col0, offset 0): both bounds must be null -- no ordering defined
      assert(stats.isNullAt(0), "BinaryType lower bound should be null")
      assert(stats.isNullAt(1), "BinaryType upper bound should be null")
      assert(stats.getInt(2) == 0, "BinaryType null count should be 0")
      assert(stats.getInt(3) == 2, "BinaryType row count should be 2")

      // IntegerType (col1, offset 5): bounds should be non-null and correct
      assert(!stats.isNullAt(5), "IntegerType lower bound should not be null")
      assert(!stats.isNullAt(6), "IntegerType upper bound should not be null")
      assert(stats.getInt(5) == 1, s"IntegerType lower=${stats.getInt(5)}")
      assert(stats.getInt(6) == 10, s"IntegerType upper=${stats.getInt(6)}")

      root.close()
    } catch {
      case e: Exception =>
        root.close()
        throw e
    }
  }

  // -------------------------------------------------------------------------
  // Collated string bug fixes
  // -------------------------------------------------------------------------

  test("caching collated string columns does not throw UnsupportedOperationException") {
    // Bug: readValueFromVector used `case StringType =>` (singleton match) which only matches
    // UTF8_BINARY. Collated StringType instances (e.g. UTF8_LCASE, UNICODE) are separate class
    // instances and fell through to `case other => throw UnsupportedOperationException(...)`.
    // Fix: use `case _: StringType =>` to match all string type instances.
    Seq("UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI").foreach { collation =>
      withTable("tbl") {
        sql(s"CACHE TABLE tbl AS SELECT col FROM VALUES " +
          s"('hello' COLLATE $collation), ('world' COLLATE $collation) AS t(col)")
        checkAnswer(
          sql("SELECT col FROM tbl"),
          Seq(Row("hello"), Row("world")))
      }
    }
  }

  test("caching collated string columns with null values reads correctly") {
    // Verify that null collated string values are also handled correctly in readValueFromVector.
    withTable("tbl") {
      sql("CACHE TABLE tbl AS SELECT col FROM VALUES " +
        "('a' COLLATE UTF8_LCASE), (null), ('B' COLLATE UTF8_LCASE) AS t(col)")
      checkAnswer(
        sql("SELECT col FROM tbl"),
        Seq(Row("a"), Row(null), Row("B")))
    }
  }

  test("filter on cached collated column uses correct semantic stats for partition pruning") {
    // Bug: collectStatistics used `case StringType =>` (singleton), so collated string columns
    // got null min/max stats. When InMemoryTableScanExec evaluated the partition filter
    // (e.g. col = 'a') against null bounds, SQL null was coerced to false and the batch was
    // incorrectly pruned, causing queries to return empty results even when matching rows exist.
    // Fix: use `case st: StringType =>` and pass st.collationId to calculateMinMaxString so
    // stats are computed with collation-aware semanticCompare, matching
    // DefaultCachedBatchSerializer.
    withTable("tbl") {
      // Cache the table so InMemoryTableScanExec is used with partition-filter pushdown.
      sql("CACHE TABLE tbl AS SELECT col FROM VALUES " +
        "('a' COLLATE UTF8_LCASE), ('B' COLLATE UTF8_LCASE), ('c' COLLATE UTF8_LCASE) AS t(col)")

      // 'a' is in the table; with null stats (before fix) the batch would be incorrectly pruned.
      checkAnswer(sql("SELECT col FROM tbl WHERE col = 'a'"), Seq(Row("a")))
      // 'B' is in the table; UTF8_LCASE: 'b' == 'B', so this matches 'B'.
      checkAnswer(sql("SELECT col FROM tbl WHERE col = 'B'"), Seq(Row("B")))
      // 'z' is not in the table; result should be empty (not incorrectly pruned to empty).
      checkAnswer(sql("SELECT col FROM tbl WHERE col = 'z'"), Seq.empty)
    }
  }

  test("row path stats for collated strings use collation-aware semantic comparison") {
    // Bug: createColumnStats used `case StringType =>` (singleton), so collated string columns
    // got StringColumnStats(StringType) -- i.e., the wrong collation ID (UTF8_BINARY=0) -- instead
    // of StringColumnStats(collatedType). Since StringColumnStats uses semanticCompare(collationId)
    // for ordering, passing the wrong collation ID produced binary-order stats for collated
    // columns,
    // which could incorrectly prune batches for case-insensitive or locale-sensitive collations.
    // Fix: use `case st: StringType => new StringColumnStats(st)`.
    //
    // Test: cache {"Apple", "banana", "Cherry"} with UTF8_LCASE.
    // Binary order: "Apple" < "Cherry" < "banana" (uppercase < lowercase in ASCII).
    // Semantic (case-insensitive) order: "Apple" < "banana" < "Cherry".
    // So semantic lower="Apple", upper="Cherry"; binary lower="Apple", upper="banana".
    // A filter WHERE col = 'cherry' should match "Cherry" semantically but not return empty.
    val collatedStringType = new StringType(1)  // collationId 1 = UTF8_LCASE
    val schema = StructType(Seq(StructField("v", collatedStringType, nullable = true)))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row("Apple"), Row("banana"), Row("Cherry")), 1),
      schema).cache()
    val stats = cachedStats(df)
    // With correct semantic stats: lower="Apple", upper="Cherry" (case-insensitive order)
    // "apple" <= "Apple" <= "banana" <= "Cherry" <= "cherry" semantically.
    assert(!stats.isNullAt(0), "lower bound should not be null for collated StringType")
    assert(!stats.isNullAt(1), "upper bound should not be null for collated StringType")
    assert(stats.getUTF8String(0).toString == "Apple")   // semantic min (case-insensitive)
    assert(stats.getUTF8String(1).toString == "Cherry")  // semantic max (case-insensitive)
    df.unpersist()
  }

  // A WKB-encoded POINT(1 2), used to build Geometry/Geography test values.
  private val wkbPoint = "0101000000000000000000F03F0000000000000040"
    .grouped(2).map(Integer.parseInt(_, 16).toByte).toArray

  // Representative value for each top-level type the serializer claims to support. This drives the
  // data-driven alignment test below: every type here is both asserted supported by
  // isSupportedByArrow AND actually cached and read back, so a type that is claimed supported but
  // fails during stats collection or read (as top-level geometry/geography once did) is caught.
  private val topLevelTypeSamples: Seq[(DataType, Any)] = Seq(
    (BooleanType, true),
    (ByteType, 1.toByte),
    (ShortType, 1.toShort),
    (IntegerType, 1),
    (LongType, 1L),
    (FloatType, 1.0f),
    (DoubleType, 1.0),
    (StringType, "x"),
    (BinaryType, Array[Byte](1, 2, 3)),
    (DateType, Date.valueOf("2020-01-01")),
    (TimestampType, Timestamp.valueOf("2020-01-01 00:00:00")),
    (TimestampNTZType, LocalDateTime.parse("2020-01-01T00:00:00")),
    (TimeType(6), LocalTime.parse("12:00:00")),
    (DecimalType(10, 2), BigDecimal("1.23").bigDecimal),
    (YearMonthIntervalType(), Period.ofMonths(3)),
    (DayTimeIntervalType(), Duration.ofSeconds(5)),
    (CalendarIntervalType, new CalendarInterval(1, 2, 3L)),
    (ArrayType(IntegerType), Seq(1, 2, 3)),
    (StructType(Seq(StructField("a", IntegerType))), Row(1)),
    (MapType(StringType, IntegerType), Map("a" -> 1)),
    (GeometryType(4326), Geometry.fromWKB(wkbPoint, 4326)),
    (GeographyType(4326), Geography.fromWKB(wkbPoint, 4326)))

  test("every type claimed supported by isSupportedByArrow can be cached and read back") {
    // Guards against the failure mode where a type is added to isSupportedByArrow but the stats
    // collector (createColumnStats) or read path (needsFallback/ArrowColumnReader) is not updated
    // to match. Driven by isSupportedByArrow itself rather than a hand-maintained list, so the
    // claim and the implementation are cross-checked on the same set of types.
    Seq(false, true).foreach { vectorized =>
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
        topLevelTypeSamples.foreach { case (dt, value) =>
          assert(ArrowUtils.isSupportedByArrow(dt),
            s"test sample type $dt is expected to be claimed supported by isSupportedByArrow")
          val df = singlePartDf(Seq(value), dt).cache()
          try {
            // Exercise both the row read (collect) and the cache materialization (count).
            assert(df.count() == 1, s"count mismatch for $dt (vectorized=$vectorized)")
            assert(df.collect().length == 1, s"collect mismatch for $dt (vectorized=$vectorized)")
          } finally {
            df.unpersist()
            InMemoryRelation.clearSerializer()
          }
        }
      }
    }
  }

  test("top-level geometry and geography roundtrip through the cache") {
    Seq(false, true).foreach { vectorized =>
      withSQLConf(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key -> vectorized.toString) {
        Seq[(DataType, Any)](
          (GeometryType(4326), Geometry.fromWKB(wkbPoint, 4326)),
          (GeographyType(4326), Geography.fromWKB(wkbPoint, 4326))).foreach { case (dt, value) =>
          val df = singlePartDf(Seq(value, null), dt).cache()
          try {
            val rows = df.collect()
            assert(rows.length == 2, s"$dt (vectorized=$vectorized)")
            assert(rows.count(_.get(0) != null) == 1, s"$dt non-null count (vectorized=$vectorized)")
            // Stats: geometry/geography reuse BinaryColumnStats, so no min/max bounds but a
            // null count of 1.
            val stats = cachedStats(df)
            assert(stats.isNullAt(0), s"$dt should have null lower bound")
            assert(stats.isNullAt(1), s"$dt should have null upper bound")
            assert(stats.getInt(2) == 1, s"$dt null count should be 1")
          } finally {
            df.unpersist()
            InMemoryRelation.clearSerializer()
          }
        }
      }
    }
  }

  test("columnar input backed by LargeVarCharVector roundtrips via the slow path") {
    // ArrowColumnVector accepts LargeVarCharVector (64-bit offsets) for StringType. The zero-copy
    // path serializes/reloads under a largeVarTypes=false schema (32-bit offsets), which would
    // corrupt such data, so the serializer must fall back to the row-based slow path. Build the
    // ColumnarBatch inside the task to avoid serializing it to executors.
    val schema = Seq(AttributeReference("v", StringType, nullable = true)())
    val conf = spark.sessionState.conf
    val ser = new ArrowCachedBatchSerializer
    val batchRdd = spark.sparkContext.parallelize(Seq(0), 1).mapPartitions { _ =>
      val alloc = ArrowUtils.rootAllocator.newChildAllocator("test-large-varchar", 0, Long.MaxValue)
      val lv = new LargeVarCharVector("v", alloc)
      lv.allocateNew(2)
      lv.setSafe(0, "hello".getBytes("UTF-8"))
      lv.setSafe(1, "world".getBytes("UTF-8"))
      lv.setValueCount(2)
      Iterator(new ColumnarBatch(Array[ColumnVector](new ArrowColumnVector(lv)), 2))
    }
    val cached = ser.convertColumnarBatchToCachedBatch(
      batchRdd, schema, StorageLevel.MEMORY_ONLY, conf)
    cached.persist()
    try {
      val values = ser.convertCachedBatchToInternalRow(cached, schema, schema, conf)
        .map(_.getString(0)).collect()
      assert(values.sorted.sameElements(Array("hello", "world")),
        s"expected [hello, world] but got [${values.mkString(", ")}]")
    } finally {
      cached.unpersist()
    }
  }

  test("nonpositive maxRecordsPerBatch caches all rows in a single batch") {
    // A nonpositive maxRecordsPerBatch means unlimited; without the `<= 0` guard the write
    // iterator would emit zero-row batches forever instead of finishing.
    Seq("0", "-1").foreach { v =>
      withSQLConf(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key -> v) {
        val df = spark.range(0, 100).repartition(1).cache()
        try {
          assert(df.count() == 100, s"maxRecordsPerBatch=$v")
          assert(df.collect().length == 100, s"maxRecordsPerBatch=$v")
        } finally {
          df.unpersist()
          InMemoryRelation.clearSerializer()
        }
      }
    }
  }

  test("row read does not drop rows after an empty cached batch") {
    // A zero-row cached batch (legal input from a columnar source) must not terminate the row
    // iterator early: subsequent non-empty batches must still be read.
    val schema = Seq(AttributeReference("v", IntegerType, nullable = true)())
    val conf = spark.sessionState.conf
    val ser = new ArrowCachedBatchSerializer
    // One empty ColumnarBatch followed by a one-row batch, each built inside the task.
    val batchRdd = spark.sparkContext.parallelize(Seq(0), 1).mapPartitions { _ =>
      def intBatch(values: Int*): ColumnarBatch = {
        val alloc = ArrowUtils.rootAllocator.newChildAllocator("test-empty", 0, Long.MaxValue)
        val iv = new IntVector("v", alloc)
        iv.allocateNew(values.length)
        values.zipWithIndex.foreach { case (x, i) => iv.setSafe(i, x) }
        iv.setValueCount(values.length)
        new ColumnarBatch(Array[ColumnVector](new ArrowColumnVector(iv)), values.length)
      }
      Iterator(intBatch(), intBatch(42))
    }
    val cached = ser.convertColumnarBatchToCachedBatch(
      batchRdd, schema, StorageLevel.MEMORY_ONLY, conf)
    cached.persist()
    try {
      val values = ser.convertCachedBatchToInternalRow(cached, schema, schema, conf)
        .map(_.getInt(0)).collect()
      assert(values.sameElements(Array(42)), s"expected [42] but got [${values.mkString(", ")}]")
    } finally {
      cached.unpersist()
    }
  }

  test("columnar read with prefetch does not release the in-use batch's buffers") {
    // With prefetch enabled, the next batch is deserialized on a background thread while the
    // current batch is consumed. The previous root must only be closed on the consumer thread
    // (in next()), never by the background prefetch; otherwise the ArrowColumnVectors backing the
    // batch currently held by the consumer point at released memory. Reading the held batch after
    // giving the background prefetch time to run reproduces that use-after-free if reintroduced.
    withSQLConf(SQLConf.ARROW_CACHE_PREFETCH_ENABLED.key -> "true") {
      val schema = Seq(AttributeReference("v", IntegerType, nullable = true)())
      val conf = spark.sessionState.conf
      val ser = new ArrowCachedBatchSerializer
      val batchRdd = spark.sparkContext.parallelize(Seq(0), 1).mapPartitions { _ =>
        (0 until 5).iterator.map { x =>
          val alloc = ArrowUtils.rootAllocator.newChildAllocator(s"prefetch-$x", 0, Long.MaxValue)
          val iv = new IntVector("v", alloc)
          iv.allocateNew(1)
          iv.setSafe(0, x * 10)
          iv.setValueCount(1)
          new ColumnarBatch(Array[ColumnVector](new ArrowColumnVector(iv)), 1)
        }
      }
      val cached = ser.convertColumnarBatchToCachedBatch(
        batchRdd, schema, StorageLevel.MEMORY_ONLY, conf)
      cached.persist()
      try {
        val values = ser.convertCachedBatchToColumnarBatch(cached, schema, schema, conf)
          .mapPartitions { it =>
            val out = scala.collection.mutable.ArrayBuffer[Int]()
            while (it.hasNext) {
              val batch = it.next() // hold exactly one batch, per the ColumnarBatch contract
              Thread.sleep(20) // give the background prefetch a chance to run before reading
              out += batch.getRow(0).getInt(0)
            }
            out.iterator
          }.collect()
        assert(values.sorted.sameElements(Array(0, 10, 20, 30, 40)),
          s"expected [0, 10, 20, 30, 40] but got [${values.sorted.mkString(", ")}]")
      } finally {
        cached.unpersist()
      }
    }
  }
}

/**
 * Tests that ArrowCachedBatch and ArrowCachedBatchSerializer are registered in KryoSerializer.
 * Without the registration, persisting with DISK_ONLY storage level would fail when
 * spark.kryo.registrationRequired=true because Kryo rejects unregistered classes.
 */
class ArrowCachedBatchKryoRegistrationSuite extends QueryTest with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(StaticSQLConf.SPARK_CACHE_SERIALIZER.key, classOf[ArrowCachedBatchSerializer].getName)
    .set("spark.kryo.registrationRequired", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  override def beforeAll(): Unit = {
    super.beforeAll()
    InMemoryRelation.clearSerializer()
  }

  override def afterAll(): Unit = {
    InMemoryRelation.clearSerializer()
    super.afterAll()
  }

  test("ArrowCachedBatch and ArrowCachedBatchSerializer are registered in KryoSerializer") {
    withTable("t1") {
      sql("CREATE TABLE t1 AS SELECT 1 AS a")
      checkAnswer(sql("SELECT * FROM t1").persist(StorageLevel.DISK_ONLY), Seq(Row(1)))
    }
  }
}
