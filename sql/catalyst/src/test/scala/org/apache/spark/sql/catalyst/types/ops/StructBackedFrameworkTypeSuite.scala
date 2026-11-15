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

package org.apache.spark.sql.catalyst.types.ops

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateMutableProjection, GenerateOrdering, GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalStructType}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test-only DataType used to validate that framework types work end-to-end through all
 * dispatch points (interpreted and codegen) by returning a raw PhysicalStructType.
 *
 * This type has 2 fields: name (String, nullable) and value (Long, non-null).
 * The internal representation is InternalRow (same as StructType).
 */
case class TestStructBackedType() extends DataType with TestableFrameworkType {
  override def defaultSize: Int = 20
  override def asNullable: TestStructBackedType = this
  override def typeName: String = "test_struct_backed"
  override def createTypeOps(): TypeOps = TestStructBackedTypeOps(this)
}

object TestStructBackedType {
  val FIELDS: Array[StructField] = Array(
    StructField("name", StringType, nullable = true),
    StructField("value", LongType, nullable = false)
  )
}

/**
 * Test-only TypeOps for TestStructBackedType.
 *
 * Returns PhysicalStructType(fields) as its physical type - this causes all row
 * infrastructure dispatchers (SpecializedGettersReader, InterpretedUnsafeProjection,
 * CodeGenerator, GenerateUnsafeProjection) to handle it as a struct automatically.
 * No PhysicalFrameworkDelegatedType or custom getValue/unsafeWrite needed.
 */
case class TestStructBackedTypeOps(override val dataType: TestStructBackedType)
    extends TypeOps {

  override def getPhysicalType: PhysicalDataType =
    PhysicalStructType(TestStructBackedType.FIELDS)

  override def getJavaClass: Class[_] = classOf[InternalRow]

  override def getMutableValue: MutableValue = new MutableAny

  override def getRowWriter(ordinal: Int): (InternalRow, Any) => Unit =
    (row, v) => row.update(ordinal, v)

  // 8 bytes: struct is variable-length, stored as offset+size pointer in UnsafeArrayData.
  override def getElementSize: Int = 8

  // Struct-backed: infrastructure code recurses through StructType branches for this type.
  override def rowRepresentationType: Option[DataType] =
    Some(StructType(TestStructBackedType.FIELDS))

  override def getDefaultLiteral: Literal =
    Literal.create(
      new GenericInternalRow(Array[Any](UTF8String.fromString(""), 0L)),
      new TestStructBackedType())

  override def getJavaLiteral(v: Any): String = v.toString

  override def toCatalystImpl(scalaValue: Any): Any = scalaValue

  override def toScala(catalystValue: Any): Any = catalystValue

  override def toScalaImpl(row: InternalRow, column: Int): Any =
    row.getStruct(column, 2)

  // Test type does not exercise client-side ops; stub TypeApiOps members so the trait contract
  // is satisfied without pulling in encoder / SQL-literal infrastructure.
  override def format(v: Any): String = v.toString
  override def toSQLValue(v: Any): String = v.toString
  override def getEncoder: AgnosticEncoder[_] =
    throw new UnsupportedOperationException("Test type does not provide an encoder")
}

/**
 * Suite validating that framework types with struct-backed physical representation work
 * end-to-end through all dispatch points - both interpreted and codegen paths.
 *
 * The key design principle: TypeOps.getPhysicalType returns the raw physical type
 * (PhysicalStructType). All dispatchers handle it automatically via PhysicalDataType
 * dispatch. No special PhysicalFrameworkDelegatedType or ops delegation needed.
 */
class StructBackedFrameworkTypeSuite extends SparkFunSuite {

  private val testType = TestStructBackedType()

  private def makeStructRow(name: String, value: Long): GenericInternalRow = {
    new GenericInternalRow(Array[Any](
      if (name != null) UTF8String.fromString(name) else null,
      value
    ))
  }

  // ==================== Physical Type Resolution ====================

  test("PhysicalDataType resolves to PhysicalStructType for struct-backed framework type") {
    val physType = PhysicalDataType(testType)
    assert(physType.isInstanceOf[PhysicalStructType])
    val st = physType.asInstanceOf[PhysicalStructType]
    assert(st.fields.length === 2)
    assert(st.fields(0).dataType === StringType)
    assert(st.fields(1).dataType === LongType)
  }

  // ==================== Interpreted Path ====================

  test("interpreted read: SpecializedGettersReader reads struct for framework type") {
    val struct = makeStructRow("hello", 42L)
    val row: SpecializedGetters = new GenericInternalRow(Array[Any](struct))

    val handleNull = true
    val handleUserDefinedType = false
    val result = SpecializedGettersReader.read(row, 0, testType, handleNull, handleUserDefinedType)
    assert(result.isInstanceOf[InternalRow])
    val resultRow = result.asInstanceOf[InternalRow]
    assert(resultRow.getUTF8String(0).toString === "hello")
    assert(resultRow.getLong(1) === 42L)
  }

  test("interpreted read: SpecializedGettersReader returns null for null column") {
    val row: SpecializedGetters = new GenericInternalRow(Array[Any](null))

    val handleNull = true
    val handleUserDefinedType = false
    val result = SpecializedGettersReader.read(row, 0, testType, handleNull, handleUserDefinedType)
    assert(result === null)
  }

  test("interpreted read: InternalRow.getAccessor dispatches through PhysicalStructType") {
    val struct = makeStructRow("accessor", 7L)
    val row = new GenericInternalRow(Array[Any](struct))

    val accessor = InternalRow.getAccessor(testType)
    val result = accessor(row, 0)
    assert(result.isInstanceOf[InternalRow])
    val resultRow = result.asInstanceOf[InternalRow]
    assert(resultRow.getUTF8String(0).toString === "accessor")
    assert(resultRow.getLong(1) === 7L)
  }

  test("interpreted write: InterpretedUnsafeProjection round-trips struct") {
    val struct = makeStructRow("test", 123L)
    val inputRow = new GenericInternalRow(Array[Any](struct))

    val exprs: Array[Expression] = Array(
      BoundReference(0, testType, nullable = true): Expression)
    val projection = new InterpretedUnsafeProjection(exprs)
    val unsafeRow = projection.apply(inputRow).copy()

    // Read back - UnsafeRow.get dispatches through PhysicalStructType
    val result = unsafeRow.getStruct(0, 2)
    assert(result.getUTF8String(0).toString === "test")
    assert(result.getLong(1) === 123L)
  }

  test("interpreted write: InterpretedUnsafeProjection handles null column") {
    val inputRow = new GenericInternalRow(Array[Any](null))

    val exprs: Array[Expression] = Array(
      BoundReference(0, testType, nullable = true): Expression)
    val projection = new InterpretedUnsafeProjection(exprs)
    val unsafeRow = projection.apply(inputRow)

    assert(unsafeRow.isNullAt(0))
  }

  // ==================== Codegen Path ====================

  test("codegen write: GenerateUnsafeProjection round-trips struct") {
    val struct = makeStructRow("codegen", 456L)
    val inputRow = new GenericInternalRow(Array[Any](struct))

    val exprs = Seq(BoundReference(0, testType, nullable = true))
    val projection = GenerateUnsafeProjection.generate(exprs)
    val unsafeRow = projection.apply(inputRow).copy()

    val result = unsafeRow.getStruct(0, 2)
    assert(result.getUTF8String(0).toString === "codegen")
    assert(result.getLong(1) === 456L)
  }

  test("codegen write: GenerateUnsafeProjection handles null column") {
    val inputRow = new GenericInternalRow(Array[Any](null))

    val exprs = Seq(BoundReference(0, testType, nullable = true))
    val projection = GenerateUnsafeProjection.generate(exprs)
    val unsafeRow = projection.apply(inputRow)

    assert(unsafeRow.isNullAt(0))
  }

  test("codegen write: GenerateUnsafeProjection round-trips null struct field") {
    val struct = makeStructRow(null, 789L)
    val inputRow = new GenericInternalRow(Array[Any](struct))

    val exprs = Seq(BoundReference(0, testType, nullable = true))
    val projection = GenerateUnsafeProjection.generate(exprs)
    val unsafeRow = projection.apply(inputRow).copy()

    val result = unsafeRow.getStruct(0, 2)
    assert(result.isNullAt(0))
    assert(result.getLong(1) === 789L)
  }

  // ==================== Literal Validation ====================

  test("literal validation accepts InternalRow for struct-backed framework type") {
    val struct = makeStructRow("lit", 10L)
    // Should not throw - PhysicalStructType validation checks InternalRow + fields
    Literal.create(struct, testType)
  }

  test("literal validation rejects non-InternalRow for struct-backed framework type") {
    intercept[IllegalArgumentException] {
      Literal.create("not a row", testType)
    }
  }

  // ==================== Array Containing Framework Type ====================

  test("interpreted write: array of framework type round-trips") {
    val elem1 = makeStructRow("a", 1L)
    val elem2 = makeStructRow("b", 2L)
    val arrayData = new GenericArrayData(Array[Any](elem1, elem2))
    val inputRow = new GenericInternalRow(Array[Any](arrayData))

    val arrayType = ArrayType(testType, containsNull = false)
    val exprs: Array[Expression] = Array(
      BoundReference(0, arrayType, nullable = false): Expression)
    val projection = new InterpretedUnsafeProjection(exprs)
    val unsafeRow = projection.apply(inputRow).copy()

    val resultArray = unsafeRow.getArray(0)
    assert(resultArray.numElements() === 2)
    val s0 = resultArray.getStruct(0, 2)
    assert(s0.getUTF8String(0).toString === "a")
    assert(s0.getLong(1) === 1L)
    val s1 = resultArray.getStruct(1, 2)
    assert(s1.getUTF8String(0).toString === "b")
    assert(s1.getLong(1) === 2L)
  }

  test("codegen write: array of framework type round-trips") {
    val elem1 = makeStructRow("x", 10L)
    val elem2 = makeStructRow("y", 20L)
    val arrayData = new GenericArrayData(Array[Any](elem1, elem2))
    val inputRow = new GenericInternalRow(Array[Any](arrayData))

    val arrayType = ArrayType(testType, containsNull = false)
    val exprs = Seq(BoundReference(0, arrayType, nullable = false))
    val projection = GenerateUnsafeProjection.generate(exprs)
    val unsafeRow = projection.apply(inputRow).copy()

    val resultArray = unsafeRow.getArray(0)
    assert(resultArray.numElements() === 2)
    val s0 = resultArray.getStruct(0, 2)
    assert(s0.getUTF8String(0).toString === "x")
    assert(s0.getLong(1) === 10L)
    val s1 = resultArray.getStruct(1, 2)
    assert(s1.getUTF8String(0).toString === "y")
    assert(s1.getLong(1) === 20L)
  }

  // ==================== Map Containing Framework Type ====================

  test("interpreted write: map with framework type values round-trips") {
    val key1 = UTF8String.fromString("k1")
    val key2 = UTF8String.fromString("k2")
    val val1 = makeStructRow("m1", 100L)
    val val2 = makeStructRow("m2", 200L)
    val mapData = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](key1, key2)),
      new GenericArrayData(Array[Any](val1, val2)))
    val inputRow = new GenericInternalRow(Array[Any](mapData))

    val mapType = MapType(StringType, testType, valueContainsNull = false)
    val exprs: Array[Expression] = Array(
      BoundReference(0, mapType, nullable = false): Expression)
    val projection = new InterpretedUnsafeProjection(exprs)
    val unsafeRow = projection.apply(inputRow).copy()

    val resultMap = unsafeRow.getMap(0)
    assert(resultMap.numElements() === 2)
    val keys = resultMap.keyArray()
    val values = resultMap.valueArray()
    assert(keys.getUTF8String(0).toString === "k1")
    assert(keys.getUTF8String(1).toString === "k2")
    val v0 = values.getStruct(0, 2)
    assert(v0.getUTF8String(0).toString === "m1")
    assert(v0.getLong(1) === 100L)
    val v1 = values.getStruct(1, 2)
    assert(v1.getUTF8String(0).toString === "m2")
    assert(v1.getLong(1) === 200L)
  }

  test("codegen write: map with framework type values round-trips") {
    val key1 = UTF8String.fromString("k1")
    val key2 = UTF8String.fromString("k2")
    val val1 = makeStructRow("m1", 100L)
    val val2 = makeStructRow("m2", 200L)
    val mapData = new ArrayBasedMapData(
      new GenericArrayData(Array[Any](key1, key2)),
      new GenericArrayData(Array[Any](val1, val2)))
    val inputRow = new GenericInternalRow(Array[Any](mapData))

    val mapType = MapType(StringType, testType, valueContainsNull = false)
    val exprs = Seq(BoundReference(0, mapType, nullable = false))
    val projection = GenerateUnsafeProjection.generate(exprs)
    val unsafeRow = projection.apply(inputRow).copy()

    val resultMap = unsafeRow.getMap(0)
    assert(resultMap.numElements() === 2)
    val keys = resultMap.keyArray()
    val values = resultMap.valueArray()
    assert(keys.getUTF8String(0).toString === "k1")
    assert(keys.getUTF8String(1).toString === "k2")
    val v0 = values.getStruct(0, 2)
    assert(v0.getUTF8String(0).toString === "m1")
    assert(v0.getLong(1) === 100L)
    val v1 = values.getStruct(1, 2)
    assert(v1.getUTF8String(0).toString === "m2")
    assert(v1.getLong(1) === 200L)
  }

  // ==================== Safe Projection ====================

  test("codegen: GenerateSafeProjection round-trips framework type") {
    val struct = makeStructRow("safe", 42L)
    val inputRow = new GenericInternalRow(Array[Any](struct))

    // Create an UnsafeRow first via GenerateUnsafeProjection
    val unsafeExprs = Seq(BoundReference(0, testType, nullable = true))
    val unsafeProjection = GenerateUnsafeProjection.generate(unsafeExprs)
    val unsafeRow = unsafeProjection.apply(inputRow)

    // Now convert back to safe via GenerateSafeProjection
    val safeExprs = Seq(BoundReference(0, testType, nullable = true))
    val safeProjection = GenerateSafeProjection.generate(safeExprs)
    val safeRow = safeProjection.apply(unsafeRow)

    // The result should be a safe (non-unsafe) InternalRow with correct values
    val result = safeRow.getStruct(0, 2)
    assert(!result.isInstanceOf[UnsafeRow])
    assert(result.getUTF8String(0).toString === "safe")
    assert(result.getLong(1) === 42L)
  }

  test("codegen: GenerateOrdering compares framework type values like structs") {
    val ordering = GenerateOrdering.generate(
      Seq(SortOrder(BoundReference(0, testType, nullable = true), Ascending)))
    def row(name: String, value: Long): InternalRow =
      new GenericInternalRow(Array[Any](makeStructRow(name, value)))
    assert(ordering.compare(row("a", 1L), row("a", 1L)) === 0)
    assert(ordering.compare(row("a", 1L), row("b", 1L)) < 0)
    assert(ordering.compare(row("a", 2L), row("a", 1L)) > 0)
  }

  test("codegen: GenerateMutableProjection sets framework type values") {
    val projection = GenerateMutableProjection.generate(
      Seq(BoundReference(0, testType, nullable = true)))
    val target = new GenericInternalRow(1)
    projection.target(target)
    projection(new GenericInternalRow(Array[Any](makeStructRow("mutable", 7L))))
    val result = target.getStruct(0, 2)
    assert(result.getUTF8String(0).toString === "mutable")
    assert(result.getLong(1) === 7L)
  }
}
