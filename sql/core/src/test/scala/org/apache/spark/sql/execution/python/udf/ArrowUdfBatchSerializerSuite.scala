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
package org.apache.spark.sql.execution.python.udf

import org.apache.arrow.memory.BufferAllocator

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Tests for ArrowUdfBatchSerializer covering round-trip serialization,
 * batch splitting, null handling, all supported Spark SQL types,
 * large batches, schema negotiation, and edge cases.
 */
class ArrowUdfBatchSerializerSuite extends SparkFunSuite {

  private var allocator: BufferAllocator = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    allocator = ArrowUdfBatchSerializer.newAllocator("test")
  }

  override def afterAll(): Unit = {
    if (allocator != null) {
      allocator.close()
    }
    super.afterAll()
  }

  private val simpleSchema = new StructType()
    .add("id", IntegerType)
    .add("value", LongType)

  private val stringSchema = new StructType()
    .add("name", StringType)
    .add("count", IntegerType)

  /** Helper: serialize with test allocator */
  private def serialize(
      rows: Iterator[InternalRow],
      schema: StructType,
      maxBatchRows: Int = 8192): Array[(Array[Byte], Long)] = {
    ArrowUdfBatchSerializer.serializeBatch(rows, schema, maxBatchRows, allocator)
  }

  /** Helper: deserialize */
  private def deserialize(
      bytes: Array[Byte],
      schema: StructType): Iterator[InternalRow] = {
    ArrowUdfBatchSerializer.deserializeBatch(bytes, schema)
  }

  // ---------------------------------------------------------------------------
  // Basic round-trip tests
  // ---------------------------------------------------------------------------

  test("round-trip serialize/deserialize simple numeric batch") {
    val rows = (0 until 100).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong * 2)): InternalRow
    }.iterator

    val batches = serialize(
      rows, simpleSchema, maxBatchRows = 8192)
    assert(batches.length == 1)
    assert(batches(0)._2 == 100)

    val deserialized = deserialize(
      batches(0)._1, simpleSchema).toArray
    assert(deserialized.length == 100)
    assert(deserialized(0).getInt(0) == 0)
    assert(deserialized(0).getLong(1) == 0L)
    assert(deserialized(99).getInt(0) == 99)
    assert(deserialized(99).getLong(1) == 198L)
  }

  test("round-trip with string data") {
    val rows = (0 until 10).map { i =>
      new GenericInternalRow(
        Array[Any](UTF8String.fromString(s"name_$i"), i)): InternalRow
    }.iterator

    val batches = serialize(rows, stringSchema)
    assert(batches.length == 1)

    val deserialized = deserialize(
      batches(0)._1, stringSchema).toArray
    assert(deserialized.length == 10)
    assert(deserialized(0).getUTF8String(0).toString == "name_0")
    assert(deserialized(9).getUTF8String(0).toString == "name_9")
  }

  test("round-trip single row") {
    val rows = Iterator(
      new GenericInternalRow(Array[Any](42, 84L)): InternalRow)

    val batches = serialize(rows, simpleSchema)
    assert(batches.length == 1)
    assert(batches(0)._2 == 1)

    val deserialized = deserialize(
      batches(0)._1, simpleSchema).toArray
    assert(deserialized.length == 1)
    assert(deserialized(0).getInt(0) == 42)
    assert(deserialized(0).getLong(1) == 84L)
  }

  // ---------------------------------------------------------------------------
  // Batch splitting
  // ---------------------------------------------------------------------------

  test("batching respects maxBatchRows") {
    val rows = (0 until 250).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong)): InternalRow
    }.iterator

    val batches = serialize(
      rows, simpleSchema, maxBatchRows = 100)
    assert(batches.length == 3)
    assert(batches(0)._2 == 100)
    assert(batches(1)._2 == 100)
    assert(batches(2)._2 == 50)
  }

  test("batch boundary exactly at maxBatchRows") {
    val rows = (0 until 200).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong)): InternalRow
    }.iterator

    val batches = serialize(
      rows, simpleSchema, maxBatchRows = 100)
    assert(batches.length == 2)
    assert(batches(0)._2 == 100)
    assert(batches(1)._2 == 100)
  }

  test("maxBatchRows = 1 produces one batch per row") {
    val rows = (0 until 5).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong)): InternalRow
    }.iterator

    val batches = serialize(
      rows, simpleSchema, maxBatchRows = 1)
    assert(batches.length == 5)
    batches.zipWithIndex.foreach { case ((_, count), idx) =>
      assert(count == 1, s"Batch $idx should have 1 row")
    }
  }

  test("data integrity across multiple batches") {
    val n = 500
    val rows = (0 until n).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong * 3)): InternalRow
    }.iterator

    val batches = serialize(
      rows, simpleSchema, maxBatchRows = 128)

    val allDeserialized = batches.flatMap { case (bytes, _) =>
      deserialize(bytes, simpleSchema).toArray
    }

    assert(allDeserialized.length == n)
    allDeserialized.zipWithIndex.foreach { case (row, i) =>
      assert(row.getInt(0) == i, s"Row $i id mismatch")
      assert(row.getLong(1) == i.toLong * 3, s"Row $i value mismatch")
    }
  }

  // ---------------------------------------------------------------------------
  // Empty / edge cases
  // ---------------------------------------------------------------------------

  test("empty iterator produces no batches") {
    val batches = serialize(
      Iterator.empty, simpleSchema)
    assert(batches.isEmpty)
  }

  test("serialized bytes are non-trivially sized") {
    val rows = (0 until 1000).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong)): InternalRow
    }.iterator

    val batches = serialize(rows, simpleSchema)
    assert(batches.length == 1)
    // 1000 rows * (4 bytes int + 8 bytes long) = 12KB minimum
    assert(batches(0)._1.length > 1000,
      "Arrow IPC bytes should be substantial for 1000 rows")
  }

  // ---------------------------------------------------------------------------
  // Null handling
  // ---------------------------------------------------------------------------

  test("round-trip with null values in nullable columns") {
    val schema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    val rows = Seq(
      new GenericInternalRow(Array[Any](1, UTF8String.fromString("a"))),
      new GenericInternalRow(Array[Any](null, UTF8String.fromString("b"))),
      new GenericInternalRow(Array[Any](3, null)),
      new GenericInternalRow(Array[Any](null, null))
    ).map(_.asInstanceOf[InternalRow]).iterator

    val batches = serialize(rows, schema)
    assert(batches.length == 1)
    assert(batches(0)._2 == 4)

    val deserialized = deserialize(
      batches(0)._1, schema).toArray
    assert(deserialized.length == 4)

    // Row 0: (1, "a")
    assert(!deserialized(0).isNullAt(0))
    assert(deserialized(0).getInt(0) == 1)
    assert(deserialized(0).getUTF8String(1).toString == "a")

    // Row 1: (null, "b")
    assert(deserialized(1).isNullAt(0))
    assert(deserialized(1).getUTF8String(1).toString == "b")

    // Row 2: (3, null)
    assert(deserialized(2).getInt(0) == 3)
    assert(deserialized(2).isNullAt(1))

    // Row 3: (null, null)
    assert(deserialized(3).isNullAt(0))
    assert(deserialized(3).isNullAt(1))
  }

  test("all-null batch") {
    val schema = new StructType()
      .add("a", IntegerType, nullable = true)
      .add("b", StringType, nullable = true)

    val rows = (0 until 10).map { _ =>
      new GenericInternalRow(Array[Any](null, null)): InternalRow
    }.iterator

    val batches = serialize(rows, schema)
    val deserialized = deserialize(
      batches(0)._1, schema).toArray

    assert(deserialized.length == 10)
    deserialized.foreach { row =>
      assert(row.isNullAt(0))
      assert(row.isNullAt(1))
    }
  }

  // ---------------------------------------------------------------------------
  // All supported Spark SQL types from the design doc type mapping table
  // ---------------------------------------------------------------------------

  test("round-trip with boolean type") {
    val schema = new StructType().add("flag", BooleanType)
    val rows = Seq(true, false, true).map { v =>
      new GenericInternalRow(Array[Any](v)): InternalRow
    }.iterator

    val batches = serialize(rows, schema)
    val deserialized = deserialize(
      batches(0)._1, schema).toArray

    assert(deserialized(0).getBoolean(0) == true)
    assert(deserialized(1).getBoolean(0) == false)
    assert(deserialized(2).getBoolean(0) == true)
  }

  test("round-trip with float and double types") {
    val schema = new StructType()
      .add("f", FloatType)
      .add("d", DoubleType)

    val rows = Seq(
      (1.5f, 2.5),
      (Float.MaxValue, Double.MinValue),
      (0.0f, 0.0)
    ).map { case (f, d) =>
      new GenericInternalRow(Array[Any](f, d)): InternalRow
    }.iterator

    val batches = serialize(rows, schema)
    val deserialized = deserialize(
      batches(0)._1, schema).toArray

    assert(deserialized.length == 3)
    assert(deserialized(0).getFloat(0) == 1.5f)
    assert(deserialized(0).getDouble(1) == 2.5)
    assert(deserialized(1).getFloat(0) == Float.MaxValue)
    assert(deserialized(1).getDouble(1) == Double.MinValue)
  }

  test("round-trip with short and byte types") {
    val schema = new StructType()
      .add("s", ShortType)
      .add("b", ByteType)

    val rows = Seq(
      (100.toShort, 42.toByte),
      (Short.MaxValue, Byte.MinValue)
    ).map { case (s, b) =>
      new GenericInternalRow(Array[Any](s, b)): InternalRow
    }.iterator

    val batches = serialize(rows, schema)
    val deserialized = deserialize(
      batches(0)._1, schema).toArray

    assert(deserialized(0).getShort(0) == 100)
    assert(deserialized(0).getByte(1) == 42)
    assert(deserialized(1).getShort(0) == Short.MaxValue)
    assert(deserialized(1).getByte(1) == Byte.MinValue)
  }

  test("round-trip with binary type") {
    val schema = new StructType().add("data", BinaryType)
    val testBytes = Array[Byte](1, 2, 3, 0, -1, 127)

    val rows = Iterator(
      new GenericInternalRow(Array[Any](testBytes)): InternalRow)

    val batches = serialize(rows, schema)
    val deserialized = deserialize(
      batches(0)._1, schema).toArray

    assert(deserialized.length == 1)
    assert(deserialized(0).getBinary(0).sameElements(testBytes))
  }

  test("round-trip with array type") {
    val schema = new StructType()
      .add("ids", ArrayType(IntegerType))

    val arr1 = ArrayData.toArrayData(Array(1, 2, 3))
    val arr2 = ArrayData.toArrayData(Array(10, 20))

    val rows = Seq(arr1, arr2).map { arr =>
      new GenericInternalRow(Array[Any](arr)): InternalRow
    }.iterator

    val batches = serialize(rows, schema)
    val deserialized = deserialize(
      batches(0)._1, schema).toArray

    assert(deserialized.length == 2)
    val result0 = deserialized(0).getArray(0)
    assert(result0.numElements() == 3)
    assert(result0.getInt(0) == 1)
    assert(result0.getInt(1) == 2)
    assert(result0.getInt(2) == 3)

    val result1 = deserialized(1).getArray(0)
    assert(result1.numElements() == 2)
    assert(result1.getInt(0) == 10)
    assert(result1.getInt(1) == 20)
  }

  // ---------------------------------------------------------------------------
  // Large batch test
  // ---------------------------------------------------------------------------

  test("large batch round-trip (64K rows)") {
    val n = 65536
    val rows = (0 until n).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong)): InternalRow
    }.iterator

    val batches = serialize(
      rows, simpleSchema, maxBatchRows = n)
    assert(batches.length == 1)
    assert(batches(0)._2 == n)

    val deserialized = deserialize(
      batches(0)._1, simpleSchema).toArray
    assert(deserialized.length == n)

    // Spot-check first, middle, last
    assert(deserialized(0).getInt(0) == 0)
    assert(deserialized(n / 2).getInt(0) == n / 2)
    assert(deserialized(n - 1).getInt(0) == n - 1)
    assert(deserialized(n - 1).getLong(1) == (n - 1).toLong)
  }

  test("large batch with adaptive splitting (default 8192)") {
    val n = 20000
    val rows = (0 until n).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong)): InternalRow
    }.iterator

    val batches = serialize(rows, simpleSchema)
    // Default maxBatchRows=8192, so 20000 rows -> 3 batches (8192+8192+3616)
    assert(batches.length == 3)
    assert(batches.map(_._2).sum == n)
  }

  // ---------------------------------------------------------------------------
  // Wide schema test
  // ---------------------------------------------------------------------------

  test("round-trip with wide schema (20 columns)") {
    val schema = new StructType(
      (0 until 20).map(i => StructField(s"col_$i", LongType)).toArray
    )

    val rows = (0 until 50).map { i =>
      val values = (0 until 20).map(c => (i * 20 + c).toLong).toArray[Any]
      new GenericInternalRow(values): InternalRow
    }.iterator

    val batches = serialize(rows, schema)
    assert(batches.length == 1)

    val deserialized = deserialize(
      batches(0)._1, schema).toArray
    assert(deserialized.length == 50)

    // Verify first and last row
    (0 until 20).foreach { c =>
      assert(deserialized(0).getLong(c) == c.toLong)
      assert(deserialized(49).getLong(c) == (49 * 20 + c).toLong)
    }
  }

  // ---------------------------------------------------------------------------
  // Schema serialization and compatibility
  // ---------------------------------------------------------------------------

  test("schema serialization round-trip") {
    val schema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType)
      .add("value", DoubleType)
      .add("flag", BooleanType)

    val bytes = ArrowUdfBatchSerializer.serializeSchema(schema)
    assert(bytes.nonEmpty)

    val recovered = ArrowUdfBatchSerializer.deserializeSchema(bytes)
    assert(recovered.length == schema.length)
    schema.zip(recovered).foreach { case (expected, actual) =>
      assert(expected.dataType == actual.dataType,
        s"Type mismatch for field ${expected.name}")
    }
  }

  test("schema serialization with complex types") {
    val schema = new StructType()
      .add("arr", ArrayType(IntegerType))
      .add("nested", new StructType()
        .add("x", DoubleType)
        .add("y", DoubleType))

    val bytes = ArrowUdfBatchSerializer.serializeSchema(schema)
    val recovered = ArrowUdfBatchSerializer.deserializeSchema(bytes)
    assert(recovered.length == 2)
  }

  test("schema compatibility - matching schemas") {
    val result = ArrowUdfBatchSerializer.validateSchemaCompatibility(
      simpleSchema, simpleSchema)
    assert(result.isEmpty)
  }

  test("schema compatibility - field count mismatch") {
    val widerSchema = simpleSchema.add("extra", StringType)
    val result = ArrowUdfBatchSerializer.validateSchemaCompatibility(
      simpleSchema, widerSchema)
    assert(result.isDefined)
    assert(result.get.contains("field count mismatch"))
  }

  test("schema compatibility - type mismatch") {
    val wrongSchema = new StructType()
      .add("id", StringType)  // was IntegerType
      .add("value", LongType)
    val result = ArrowUdfBatchSerializer.validateSchemaCompatibility(
      simpleSchema, wrongSchema)
    assert(result.isDefined)
    assert(result.get.contains("Field 0"))
  }

  test("schema compatibility - multiple type mismatches reported") {
    val wrongSchema = new StructType()
      .add("id", StringType)
      .add("value", DoubleType)
    val result = ArrowUdfBatchSerializer.validateSchemaCompatibility(
      simpleSchema, wrongSchema)
    assert(result.isDefined)
    assert(result.get.contains("Field 0"))
    assert(result.get.contains("Field 1"))
  }

  test("schema compatibility - same types different names is compatible") {
    val renamedSchema = new StructType()
      .add("key", IntegerType)
      .add("amount", LongType)
    val result = ArrowUdfBatchSerializer.validateSchemaCompatibility(
      simpleSchema, renamedSchema)
    // Names don't matter for type compatibility
    assert(result.isEmpty)
  }

  // ---------------------------------------------------------------------------
  // Consistency: serialize produces deterministic output
  // ---------------------------------------------------------------------------

  test("serializing same data twice produces same-length output") {
    def makeRows(): Iterator[InternalRow] = (0 until 100).map { i =>
      new GenericInternalRow(Array[Any](i, i.toLong)): InternalRow
    }.iterator

    val batches1 = serialize(makeRows(), simpleSchema)
    val batches2 = serialize(makeRows(), simpleSchema)

    assert(batches1.length == batches2.length)
    assert(batches1(0)._1.length == batches2(0)._1.length)
    assert(batches1(0)._2 == batches2(0)._2)
  }
}
