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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, GenerateMutableProjection, GenerateOrdering, GeneratePredicate, GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.objects.{CatalystToExternalMap, MapObjects, UnresolvedCatalystToExternalMap}
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalStructType}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Test-only DataType backed by a struct: its physical type is a PhysicalStructType and its row
 * representation is the matching StructType.
 *
 * This type has 2 fields: name (String, nullable) and value (Long, non-null).
 * The internal representation is InternalRow (same as StructType).
 */
case class TestStructBackedType() extends TestableFrameworkType {
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
 * The sites that dispatch on the physical type (SpecializedGettersReader,
 * InternalRow.getAccessor, InterpretedUnsafeProjection's field writers) handle the type through
 * PhysicalStructType(fields). The sites that dispatch on the logical type (the code generation
 * helpers, InternalRow.getWriter and InterpretedSafeProjection) handle it through its row
 * representation, StructType(fields).
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
 * A user-defined type stored as TestStructBackedType, to check that the dispatch sites look a
 * framework type up again after unwrapping a UDT.
 */
class TestStructBackedUDT extends UserDefinedType[InternalRow] {
  override def sqlType: DataType = TestStructBackedType()
  override def serialize(obj: InternalRow): Any = obj
  override def deserialize(datum: Any): InternalRow = datum.asInstanceOf[InternalRow]
  override def userClass: Class[InternalRow] = classOf[InternalRow]
}

/**
 * Suite for a framework type with a struct-backed physical representation, on the interpreted
 * and the code generation paths.
 *
 * The readers and the interpreted unsafe writer reach the type through its physical type
 * (PhysicalStructType). The code generation helpers, InternalRow.getWriter and
 * InterpretedSafeProjection reach it through its row representation (StructType).
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
  // ==================== Layout Declarations ====================

  test("registered framework types declare a consistent layout") {
    val types = Seq(TimeType(6), TimestampNTZNanosType(9), TimestampLTZNanosType(9), testType)
    types.foreach { dt =>
      val ops = TypeOps(dt).get
      ops.rowRepresentationType.foreach { rt =>
        assert(TypeOps(rt).isEmpty, s"$dt: the row representation $rt must be a built-in type")
        (PhysicalDataType(rt), ops.getPhysicalType) match {
          case (PhysicalStructType(a), PhysicalStructType(b)) => assert(a.toSeq === b.toSeq)
          case (a, b) => assert(a === b, s"$dt: the row representation $rt has another layout")
        }
      }
      // The slot size GenerateUnsafeProjection uses for an array element of this type.
      val slotSize = if (CodeGenerator.isPrimitiveType(dt)) dt.defaultSize else 8
      assert(ops.getElementSize === slotSize, s"$dt")
    }
  }

  // ==================== Reused UnsafeRow Input ====================

  /**
   * Runs `f` on an UnsafeRow holding `first`, then writes `second` into the same buffer, so
   * that a result of `f` still pointing into the buffer reads `second` afterwards.
   */
  private def withReusedUnsafeInput[T](dt: DataType, first: Any, second: Any)(
      f: UnsafeRow => T): T = {
    val producer = UnsafeProjection.create(Array(dt))
    val result = f(producer(new GenericInternalRow(Array[Any](first))))
    producer(new GenericInternalRow(Array[Any](second)))
    result
  }

  private def assertStruct(row: InternalRow, name: String, value: Long): Unit = {
    assert(row.getUTF8String(0).toString === name)
    assert(row.getLong(1) === value)
  }

  Seq(
    "a struct-backed framework type" -> TestStructBackedType(),
    "a UDT over a struct-backed framework type" -> new TestStructBackedUDT).foreach {
    case (desc, dt) => Seq(true, false).foreach { codegen =>
      test(s"safe projection copies $desc out of a reused UnsafeRow (codegen=$codegen)") {
        val exprs = Seq(BoundReference(0, dt, nullable = true))
        val projection = if (codegen) {
          GenerateSafeProjection.generate(exprs)
        } else {
          InterpretedSafeProjection.createProjection(exprs)
        }
        val result =
          withReusedUnsafeInput(dt, makeStructRow("one", 1L), makeStructRow("two", 2L)) {
            input => projection(input).getStruct(0, 2)
          }
        assert(!result.isInstanceOf[UnsafeRow])
        assertStruct(result, "one", 1L)
      }

      test(s"mutable projection copies $desc out of a reused UnsafeRow (codegen=$codegen)") {
        val exprs = Seq(BoundReference(0, dt, nullable = true))
        val projection = if (codegen) {
          GenerateMutableProjection.generate(exprs)
        } else {
          InterpretedMutableProjection.createProjection(exprs)
        }
        val target = new GenericInternalRow(1)
        projection.target(target)
        withReusedUnsafeInput(dt, makeStructRow("one", 1L), makeStructRow("two", 2L)) {
          input => projection(input)
        }
        assertStruct(target.getStruct(0, 2), "one", 1L)
      }

      test(s"unsafe projection round-trips an array of $desc (codegen=$codegen)") {
        val arrayType = ArrayType(dt, containsNull = false)
        val exprs = Seq(BoundReference(0, arrayType, nullable = false))
        val projection = if (codegen) {
          GenerateUnsafeProjection.generate(exprs)
        } else {
          InterpretedUnsafeProjection.createProjection(exprs)
        }
        val input = new GenericInternalRow(Array[Any](
          new GenericArrayData(Array[Any](makeStructRow("a", 1L), makeStructRow("b", 2L)))))
        val array = projection(input).getArray(0)
        assert(array.numElements() === 2)
        assertStruct(array.getStruct(0, 2), "a", 1L)
        assertStruct(array.getStruct(1, 2), "b", 2L)
      }
    }
  }

  Seq(
    "a struct-backed framework type" -> TestStructBackedType(),
    "a UDT over a struct-backed framework type" -> new TestStructBackedUDT).foreach {
    case (desc, dt) =>
      test(s"codegen: MapObjects copies $desc out of a reused UnsafeRow") {
        val arrayType = ArrayType(dt, containsNull = false)
        val mapObjects = MapObjects(
          e => e, BoundReference(0, arrayType, nullable = false), dt,
          elementNullable = false, customCollectionCls = Some(classOf[java.util.ArrayList[_]]))
        val projection = GenerateSafeProjection.generate(Seq(mapObjects))
        def array(name: String, value: Long): GenericArrayData =
          new GenericArrayData(Array[Any](makeStructRow(name, value)))
        val result = withReusedUnsafeInput(arrayType, array("one", 1L), array("two", 2L)) {
          input =>
            projection(input).get(0, mapObjects.dataType)
              .asInstanceOf[java.util.List[InternalRow]]
        }
        assertStruct(result.get(0), "one", 1L)
      }

      test(s"codegen: CatalystToExternalMap copies $desc out of a reused UnsafeRow") {
        val mapType = MapType(IntegerType, dt, valueContainsNull = false)
        val toMap = CatalystToExternalMap(UnresolvedCatalystToExternalMap(
          BoundReference(0, mapType, nullable = false), k => k, v => v,
          classOf[scala.collection.immutable.Map[_, _]]))
        val projection = GenerateSafeProjection.generate(Seq(toMap))
        def map(name: String, value: Long): ArrayBasedMapData = new ArrayBasedMapData(
          new GenericArrayData(Array[Any](1)),
          new GenericArrayData(Array[Any](makeStructRow(name, value))))
        val result = withReusedUnsafeInput(mapType, map("one", 1L), map("two", 2L)) { input =>
          projection(input).get(0, toMap.dataType).asInstanceOf[Map[Int, InternalRow]]
        }
        assertStruct(result(1), "one", 1L)
      }

      Seq(true, false).foreach { codegen =>
        test(s"safe projection keeps null values of $desc nested in an array, a map and a " +
            s"struct (codegen=$codegen)") {
          val arrayType = ArrayType(dt, containsNull = true)
          val mapType = MapType(IntegerType, dt, valueContainsNull = true)
          val structType = new StructType().add("f", dt, nullable = true)
          val types = Seq[DataType](arrayType, mapType, structType)
          val exprs = types.zipWithIndex.map { case (t, i) =>
            BoundReference(i, t, nullable = true)
          }
          val projection = if (codegen) {
            GenerateSafeProjection.generate(exprs)
          } else {
            InterpretedSafeProjection.createProjection(exprs)
          }
          val input = UnsafeProjection.create(types.toArray).apply(
            new GenericInternalRow(Array[Any](
              new GenericArrayData(Array[Any](makeStructRow("a", 1L), null)),
              new ArrayBasedMapData(
                new GenericArrayData(Array[Any](1, 2)),
                new GenericArrayData(Array[Any](makeStructRow("m", 2L), null))),
              new GenericInternalRow(Array[Any](null)))))
          val result = projection(input)

          val array = result.getArray(0)
          assertStruct(array.getStruct(0, 2), "a", 1L)
          assert(array.isNullAt(1))
          val values = result.getMap(1).valueArray()
          assertStruct(values.getStruct(0, 2), "m", 2L)
          assert(values.isNullAt(1))
          assert(result.getStruct(2, 1).isNullAt(0))
        }
      }
  }

  // ==================== Other Code Generation Helpers ====================

  test("codegen: equality compares framework type values like structs") {
    val predicate = GeneratePredicate.generate(
      EqualTo(BoundReference(0, testType, nullable = true),
        BoundReference(1, testType, nullable = true)))
    def row(a: InternalRow, b: InternalRow): InternalRow = new GenericInternalRow(Array[Any](a, b))
    assert(predicate.eval(row(makeStructRow("a", 1L), makeStructRow("a", 1L))))
    assert(!predicate.eval(row(makeStructRow("a", 1L), makeStructRow("b", 1L))))
    assert(!predicate.eval(row(makeStructRow("a", 1L), makeStructRow("a", 2L))))
  }

  test("codegen: addBufferedState copies framework type values") {
    Seq(testType, new TestStructBackedUDT).foreach { dt =>
      val state = new CodegenContext().addBufferedState(dt, "buffered", "input")
      assert(state.code.toString.contains("input.copy()"), s"$dt")
    }
  }

  test("GenerateUnsafeProjection.canSupport accepts framework types and types holding them") {
    Seq[DataType](
      testType,
      new TestStructBackedUDT,
      ArrayType(testType),
      MapType(testType, testType),
      new StructType().add("f", testType)).foreach { dt =>
      assert(GenerateUnsafeProjection.canSupport(dt), s"$dt")
    }
  }
}
