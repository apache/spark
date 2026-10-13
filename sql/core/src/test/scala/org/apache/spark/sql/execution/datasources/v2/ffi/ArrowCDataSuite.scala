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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.nio.charset.StandardCharsets.UTF_8

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, LargeVarBinaryVector, LargeVarCharVector, VectorSchemaRoot, ViewVarBinaryVector, ViewVarCharVector}
import org.apache.arrow.vector.compare.VectorEqualsVisitor
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, IntervalUnit, TimeUnit, UnionMode}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.arrow.ArrowWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.Platform

/**
 * Tests the Arrow C data interface of [[ArrowCData]], by exporting and importing in the JVM. The
 * tests are skipped before Java 22, where the FFM API is not available.
 */
class ArrowCDataSuite extends SparkFunSuite {

  private def ffmTest(name: String)(f: => Unit): Unit = test(name) {
    assume(NativeLibrary.isSupported, "Java 22 or later is required.")
    f
  }

  /** Runs `f` with an allocator, and checks that all its memory is released afterwards. */
  private def withAllocator[T](f: BufferAllocator => T): T = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("test", 0, Long.MaxValue)
    try {
      val result = f(allocator)
      assert(allocator.getAllocatedMemory == 0, "Memory was not released.")
      result
    } finally {
      allocator.close()
    }
  }

  private def withStruct[T](size: Long)(f: Long => T): T = {
    val address = ArrowCData.allocate(size)
    try f(address) finally ArrowCData.free(address)
  }

  /** Exports and imports a schema. */
  private def roundTrip(schema: Schema): Schema = withStruct(ArrowCData.SCHEMA_SIZE) { struct =>
    ArrowCData.exportSchema(schema, struct)
    ArrowCData.importSchema(struct)
  }

  /**
   * Exports the root, imports it into a new root with the imported schema, and passes the new
   * root to `f`. The exported root is closed first, so the imported buffers must stay valid.
   */
  private def roundTrip[T](allocator: BufferAllocator, root: VectorSchemaRoot)(
      f: VectorSchemaRoot => T): T = {
    val imported = VectorSchemaRoot.create(roundTrip(root.getSchema), allocator)
    try {
      withStruct(ArrowCData.ARRAY_SIZE) { array =>
        ArrowCData.exportRoot(allocator, root, array)
        ArrowCData.importIntoRoot(allocator, array, imported)
        assert(ArrowCData.isArrayReleased(array), "The array was not moved.")
      }
      f(imported)
    } finally {
      imported.close()
    }
  }

  private def assertSameVectors(expected: VectorSchemaRoot, actual: VectorSchemaRoot): Unit = {
    assert(actual.getRowCount == expected.getRowCount)
    expected.getFieldVectors.asScala.zip(actual.getFieldVectors.asScala).foreach {
      case (e, a) => assert(VectorEqualsVisitor.vectorEquals(e, a), s"Different ${e.getName}.")
    }
  }

  private def rows(root: VectorSchemaRoot, schema: StructType): Seq[Row] = {
    val vectors = root.getFieldVectors.asScala.map(v => new ArrowColumnVector(v): ColumnVector)
    val batch = new ColumnarBatch(vectors.toArray, root.getRowCount)
    val toScala = CatalystTypeConverters.createToScalaConverter(schema)
    // Converted without copying the rows: ColumnarArray.copy fails on primitive arrays with nulls.
    batch.rowIterator().asScala.map(row => toScala(row).asInstanceOf[Row]).toList
  }

  private val schema = StructType(Seq(
    StructField("boolean", BooleanType),
    StructField("byte", ByteType),
    StructField("short", ShortType),
    StructField("int", IntegerType, nullable = false),
    StructField("long", LongType),
    StructField("float", FloatType),
    StructField("double", DoubleType),
    StructField("decimal", DecimalType(10, 2)),
    StructField("wide_decimal", DecimalType(38, 18)),
    StructField("string", StringType),
    StructField("binary", BinaryType),
    StructField("date", DateType),
    StructField("timestamp", TimestampType),
    StructField("timestamp_ntz", TimestampNTZType),
    StructField("year_month", YearMonthIntervalType()),
    StructField("day_time", DayTimeIntervalType()),
    StructField("null", NullType),
    StructField("ints", ArrayType(IntegerType)),
    StructField("strings", ArrayType(StringType, containsNull = false)),
    StructField("map", MapType(StringType, IntegerType)),
    StructField("nested_map", MapType(IntegerType, ArrayType(LongType))),
    StructField("struct", StructType(Seq(
      StructField("a", IntegerType), StructField("b", StringType)))),
    StructField("structs", ArrayType(StructType(Seq(
      StructField("x", DoubleType), StructField("y", ArrayType(StringType))))))))

  private def write(allocator: BufferAllocator, schema: StructType, rows: Seq[Row])
      : VectorSchemaRoot = {
    val root = VectorSchemaRoot.create(
      ArrowUtils.toArrowSchema(schema, "UTC", errorOnDuplicatedFieldNames = true,
        largeVarTypes = false),
      allocator)
    val writer = ArrowWriter.create(root)
    val toCatalyst = CatalystTypeConverters.createToCatalystConverter(schema)
    rows.foreach(row => writer.write(toCatalyst(row).asInstanceOf[InternalRow]))
    writer.finish()
    root
  }

  private def randomRows(schema: StructType, count: Int): Seq[Row] = {
    val random = new Random(42)
    Seq.fill(count)(RandomDataGenerator.randomRow(random, schema))
  }

  ffmTest("round trip of schemas") {
    // scalastyle:off nonascii
    val metadata = Map("key" -> "value", "unicode \u00e9" -> "\u4e2d").asJava
    // scalastyle:on nonascii
    val arrowSchema = new Schema(Seq(
      new Field("a", new FieldType(true, new ArrowType.Int(32, true), null, metadata), null),
      new Field("m", FieldType.notNullable(new ArrowType.Map(true)), Seq(
        new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), Seq(
          new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
          new Field("value", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)).asJava)).asJava),
      new Field("ts", FieldType.nullable(
        new ArrowType.Timestamp(TimeUnit.MICROSECOND, "America/Los_Angeles")), null)).asJava,
      Map("schema" -> "metadata").asJava)
    assert(roundTrip(arrowSchema) == arrowSchema)
    val sparkSchema = ArrowUtils.toArrowSchema(
      schema, "UTC", errorOnDuplicatedFieldNames = true, largeVarTypes = false)
    assert(roundTrip(sparkSchema) == sparkSchema)
  }

  ffmTest("round trip of arrays of the Spark types") {
    withAllocator { allocator =>
      val expected = randomRows(schema, 100)
      val root = write(allocator, schema, expected)
      try {
        roundTrip(allocator, root) { imported =>
          assertSameVectors(root, imported)
          assert(rows(imported, schema) == expected)
        }
      } finally {
        root.close()
      }
    }
  }

  ffmTest("the imported buffers outlive the exported vectors") {
    withAllocator { allocator =>
      val schema = StructType(Seq(StructField("s", StringType), StructField("l", LongType)))
      val expected = randomRows(schema, 50)
      val root = write(allocator, schema, expected)
      val imported = VectorSchemaRoot.create(roundTrip(root.getSchema), allocator)
      try {
        withStruct(ArrowCData.ARRAY_SIZE) { array =>
          ArrowCData.exportRoot(allocator, root, array)
          // The array retains the buffers of the vectors.
          root.close()
          ArrowCData.importIntoRoot(allocator, array, imported)
        }
        assert(rows(imported, schema) == expected)
      } finally {
        imported.close()
      }
    }
  }

  ffmTest("round trip of empty arrays") {
    withAllocator { allocator =>
      val root = write(allocator, schema, Nil)
      try {
        roundTrip(allocator, root) { imported =>
          assert(imported.getRowCount == 0)
          assertSameVectors(root, imported)
        }
      } finally {
        root.close()
      }
    }
  }

  ffmTest("round trip of views and large variable-width types") {
    withAllocator { allocator =>
      // scalastyle:off nonascii
      val values = Seq("short", null, "a string longer than twelve bytes", "", "\u00e9\u4e2d")
      // scalastyle:on nonascii
      val utf8View = new ViewVarCharVector("utf8_view", allocator)
      val binaryView = new ViewVarBinaryVector("binary_view", allocator)
      val largeUtf8 = new LargeVarCharVector("large_utf8", allocator)
      val largeBinary = new LargeVarBinaryVector("large_binary", allocator)
      val vectors = Seq[FieldVector](utf8View, binaryView, largeUtf8, largeBinary)
      vectors.foreach(_.allocateNew())
      values.zipWithIndex.foreach {
        case (null, i) =>
          utf8View.setNull(i)
          binaryView.setNull(i)
          largeUtf8.setNull(i)
          largeBinary.setNull(i)
        case (value, i) =>
          val bytes = value.getBytes(UTF_8)
          utf8View.setSafe(i, bytes)
          binaryView.setSafe(i, bytes)
          largeUtf8.setSafe(i, bytes)
          largeBinary.setSafe(i, bytes)
      }
      vectors.foreach(_.setValueCount(values.length))
      val root = new VectorSchemaRoot(vectors.asJava)
      try {
        roundTrip(allocator, root) { imported =>
          assertSameVectors(root, imported)
          val read = rows(imported, StructType(Seq(
            StructField("utf8_view", StringType), StructField("binary_view", BinaryType),
            StructField("large_utf8", StringType), StructField("large_binary", BinaryType))))
          assert(read.map(_.getString(0)) == values)
          assert(read.map(_.getString(2)) == values)
        }
      } finally {
        root.close()
      }
    }
  }

  ffmTest("release a child that the consumer moved") {
    withAllocator { allocator =>
      val schema = StructType(Seq(StructField("a", LongType), StructField("b", StringType)))
      val root = write(allocator, schema, randomRows(schema, 10))
      try {
        withStruct(ArrowCData.ARRAY_SIZE) { array =>
          ArrowCData.exportRoot(allocator, root, array)
          withStruct(ArrowCData.ARRAY_SIZE) { moved =>
            // Moves the first child, as the C data interface allows: copies it, and marks the
            // original as released, by setting its release callback, at offset 64, to NULL.
            val child = ArrowCData.child(array, 0)
            Platform.copyMemory(null, child, null, moved, ArrowCData.ARRAY_SIZE)
            Platform.putLong(null, child + 64, 0L)
            ArrowCData.releaseArray(array)
            assert(ArrowCData.isArrayReleased(array))
            assert(!ArrowCData.isArrayReleased(moved))
            root.close()
            // The moved child still retains its buffers.
            assert(allocator.getAllocatedMemory > 0)
            ArrowCData.releaseArray(moved)
            assert(ArrowCData.isArrayReleased(moved))
          }
        }
      } finally {
        root.close()
      }
    }
  }

  ffmTest("formats") {
    val types = Seq(
      ArrowType.Null.INSTANCE,
      ArrowType.Bool.INSTANCE,
      new ArrowType.Int(8, true), new ArrowType.Int(8, false),
      new ArrowType.Int(16, true), new ArrowType.Int(16, false),
      new ArrowType.Int(32, true), new ArrowType.Int(32, false),
      new ArrowType.Int(64, true), new ArrowType.Int(64, false),
      new ArrowType.FloatingPoint(FloatingPointPrecision.HALF),
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
      ArrowType.Binary.INSTANCE, ArrowType.LargeBinary.INSTANCE, ArrowType.BinaryView.INSTANCE,
      ArrowType.Utf8.INSTANCE, ArrowType.LargeUtf8.INSTANCE, ArrowType.Utf8View.INSTANCE,
      new ArrowType.FixedSizeBinary(5),
      new ArrowType.Decimal(10, 2, 128), new ArrowType.Decimal(40, 5, 256),
      new ArrowType.Date(DateUnit.DAY), new ArrowType.Date(DateUnit.MILLISECOND),
      new ArrowType.Time(TimeUnit.SECOND, 32), new ArrowType.Time(TimeUnit.MILLISECOND, 32),
      new ArrowType.Time(TimeUnit.MICROSECOND, 64), new ArrowType.Time(TimeUnit.NANOSECOND, 64),
      new ArrowType.Interval(IntervalUnit.YEAR_MONTH),
      new ArrowType.Interval(IntervalUnit.DAY_TIME),
      new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO),
      ArrowType.List.INSTANCE, ArrowType.LargeList.INSTANCE,
      ArrowType.ListView.INSTANCE, ArrowType.LargeListView.INSTANCE,
      new ArrowType.FixedSizeList(3),
      ArrowType.Struct.INSTANCE,
      new ArrowType.Map(false)) ++
      TimeUnit.values().toSeq.flatMap { unit =>
        Seq(new ArrowType.Timestamp(unit, null), new ArrowType.Timestamp(unit, "UTC"),
          new ArrowType.Duration(unit))
      }
    types.foreach { arrowType =>
      assert(ArrowCData.parseFormat(ArrowCData.format(arrowType), 0) == arrowType)
    }
    // The flags of a map say whether its keys are sorted.
    assert(ArrowCData.parseFormat("+m", 4) == new ArrowType.Map(true))
    assert(ArrowCData.parseFormat("+ud:1,2", 0) ==
      new ArrowType.Union(UnionMode.Dense, Array(1, 2)))
    assert(ArrowCData.parseFormat("tsu:+01:00", 0) ==
      new ArrowType.Timestamp(TimeUnit.MICROSECOND, "+01:00"))
    Seq("", "x", "d:1", "d:a,b", "w:x", "+w:", "tsx:", "tsu").foreach { format =>
      val e = intercept[IllegalArgumentException](ArrowCData.parseFormat(format, 0))
      assert(e.getMessage == s"Unsupported Arrow format: $format")
    }
  }

  /**
   * Builds ArrowSchema structs by hand, without release callbacks, so that importing them does not
   * release them: the memory is freed afterwards with `free`. The fields are at the offsets of the
   * struct: format 0, name 8, metadata 16, flags 24, n_children 32, children 40, dictionary 48.
   */
  private class RawSchemas {
    private val allocations = scala.collection.mutable.ArrayBuffer.empty[Long]

    private def allocate(size: Long): Long = {
      val address = ArrowCData.allocate(size)
      allocations += address
      address
    }

    private def string(value: String): Long = {
      val bytes = value.getBytes(UTF_8)
      val address = allocate(bytes.length + 1)
      Platform.copyMemory(bytes, Platform.BYTE_ARRAY_OFFSET, null, address, bytes.length)
      address
    }

    def schema(
        format: String,
        name: String,
        children: Seq[Long] = Nil,
        dictionary: Long = 0,
        flags: Long = 2): Long = {
      val address = allocate(ArrowCData.SCHEMA_SIZE)
      Platform.putLong(null, address, string(format))
      Platform.putLong(null, address + 8, string(name))
      Platform.putLong(null, address + 24, flags)
      Platform.putLong(null, address + 32, children.length)
      if (children.nonEmpty) {
        val pointers = allocate(8L * children.length)
        children.zipWithIndex.foreach { case (child, i) =>
          Platform.putLong(null, pointers + 8L * i, child)
        }
        Platform.putLong(null, address + 40, pointers)
      }
      Platform.putLong(null, address + 48, dictionary)
      address
    }

    def free(): Unit = allocations.foreach(ArrowCData.free)
  }

  ffmTest("import dictionary-encoded fields") {
    val raw = new RawSchemas
    try {
      val dictionary = raw.schema("u", "")
      val struct = raw.schema("+s", "", Seq(raw.schema("i", "x", dictionary = dictionary,
        flags = 3)), flags = 0)
      val field = ArrowCData.importSchema(struct).getFields.get(0)
      assert(field.getName == "x")
      assert(field.getDictionary != null)
      assert(field.getDictionary.isOrdered)
      assert(field.getDictionary.getIndexType == new ArrowType.Int(32, true))
    } finally {
      raw.free()
    }
  }

  ffmTest("reject invalid schemas") {
    val raw = new RawSchemas
    try {
      def checkInvalid(schema: Long, message: String): Unit = {
        val e = intercept[IllegalArgumentException](ArrowCData.importSchema(schema))
        assert(e.getMessage == message)
      }
      checkInvalid(raw.schema("l", "x"),
        "The schema is not a struct type, but Int(64, true).")
      checkInvalid(raw.schema("+s", "", Seq(raw.schema("?", "x"))),
        "Unsupported Arrow format: ?")
      val dictionary = raw.schema("u", "")
      checkInvalid(raw.schema("+s", "", Seq(raw.schema("u", "x", dictionary = dictionary))),
        "The indices of the dictionary of x are not integers: u.")
      var nested = raw.schema("i", "x")
      (0 to 64).foreach(_ => nested = raw.schema("+s", "s", Seq(nested)))
      checkInvalid(nested, "The schema is nested more than 64 levels deep.")
    } finally {
      raw.free()
    }
  }

  ffmTest("reject invalid arrays") {
    withAllocator { allocator =>
      val schema = StructType(Seq(StructField("a", IntegerType), StructField("s", StringType)))
      // The mutations keep what the release callback of the array frees, so nothing leaks.
      def checkInvalid(message: String, importInto: Option[Schema] = None)(
          mutate: Long => Unit): Unit = {
        val root = write(allocator, schema, Seq(Row(1, "a"), Row(2, "b")))
        val imported = VectorSchemaRoot.create(importInto.getOrElse(root.getSchema), allocator)
        try {
          withStruct(ArrowCData.ARRAY_SIZE) { array =>
            ArrowCData.exportRoot(allocator, root, array)
            mutate(array)
            val e = intercept[IllegalArgumentException] {
              ArrowCData.importIntoRoot(allocator, array, imported)
            }
            assert(e.getMessage == message)
            assert(ArrowCData.isArrayReleased(array), "The array was not moved.")
          }
        } finally {
          imported.close()
          root.close()
        }
      }
      val oneColumn = new Schema(Seq(new Field("a", FieldType.nullable(
        new ArrowType.Int(32, true)), null)).asJava)
      checkInvalid("The array has 2 columns, but the schema has 1.", Some(oneColumn))(_ => ())
      checkInvalid("The array has an offset of 1, but only arrays with an offset of 0 are " +
          "supported.") { array =>
        Platform.putLong(null, ArrowCData.child(array, 0) + 16, 1L)
      }
      checkInvalid("The array has 1 nulls, but no validity buffer.") { array =>
        val column = ArrowCData.child(array, 0)
        Platform.putLong(null, column + 8, 1L)
        Platform.putLong(null, Platform.getLong(null, column + 40), 0L)
      }
      checkInvalid("The array of type Utf8 has 2 buffers, but 3 are expected.") { array =>
        Platform.putLong(null, ArrowCData.child(array, 1) + 24, 2L)
      }
      checkInvalid("The data buffer is NULL.") { array =>
        val column = ArrowCData.child(array, 1)
        Platform.putLong(null, Platform.getLong(null, column + 40) + 16, 0L)
      }
    }
  }
}
