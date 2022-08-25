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

package org.apache.spark.sql.catalyst.expressions.codegen

import java.nio.charset.StandardCharsets

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A test suite for generated projections
 */
class GeneratedProjectionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("generated projections on wider table") {
    val N = 1000
    val wideRow1 = new GenericInternalRow((0 until N).toArray[Any])
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    val wideRow2 = new GenericInternalRow(
      (0 until N).map(i => UTF8String.fromString(i.toString)).toArray[Any])
    val schema2 = StructType((1 to N).map(i => StructField("", StringType)))
    val joined = new JoinedRow(wideRow1, wideRow2)
    val joinedSchema = StructType(schema1 ++ schema2)
    val nested = new JoinedRow(InternalRow(joined, joined), joined)
    val nestedSchema = StructType(
      Seq(StructField("", joinedSchema), StructField("", joinedSchema)) ++ joinedSchema)

    // test generated UnsafeProjection
    val unsafeProj = UnsafeProjection.create(nestedSchema)
    val unsafe: UnsafeRow = unsafeProj(nested)
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === unsafe.getInt(i + 2))
      assert(s === unsafe.getUTF8String(i + 2 + N))
      assert(i === unsafe.getStruct(0, N * 2).getInt(i))
      assert(s === unsafe.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === unsafe.getStruct(1, N * 2).getInt(i))
      assert(s === unsafe.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated SafeProjection
    val safeProj = SafeProjection.create(nestedSchema)
    val result = safeProj(unsafe)
    // Can't compare GenericInternalRow with JoinedRow directly
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === result.getInt(i + 2))
      assert(s === result.getUTF8String(i + 2 + N))
      assert(i === result.getStruct(0, N * 2).getInt(i))
      assert(s === result.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === result.getStruct(1, N * 2).getInt(i))
      assert(s === result.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated MutableProjection
    val exprs = nestedSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val mutableProj = GenerateMutableProjection.generate(exprs)
    val row1 = mutableProj(result)
    assert(result === row1)
    val row2 = mutableProj(result)
    assert(result === row2)
  }

  test("SPARK-18016: generated projections on wider table requiring class-splitting") {
    val N = 4000
    val wideRow1 = new GenericInternalRow((0 until N).toArray[Any])
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    val wideRow2 = new GenericInternalRow(
      (0 until N).map(i => UTF8String.fromString(i.toString)).toArray[Any])
    val schema2 = StructType((1 to N).map(i => StructField("", StringType)))
    val joined = new JoinedRow(wideRow1, wideRow2)
    val joinedSchema = StructType(schema1 ++ schema2)
    val nested = new JoinedRow(InternalRow(joined, joined), joined)
    val nestedSchema = StructType(
      Seq(StructField("", joinedSchema), StructField("", joinedSchema)) ++ joinedSchema)

    // test generated UnsafeProjection
    val unsafeProj = UnsafeProjection.create(nestedSchema)
    val unsafe: UnsafeRow = unsafeProj(nested)
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === unsafe.getInt(i + 2))
      assert(s === unsafe.getUTF8String(i + 2 + N))
      assert(i === unsafe.getStruct(0, N * 2).getInt(i))
      assert(s === unsafe.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === unsafe.getStruct(1, N * 2).getInt(i))
      assert(s === unsafe.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated SafeProjection
    val safeProj = SafeProjection.create(nestedSchema)
    val result = safeProj(unsafe)
    // Can't compare GenericInternalRow with JoinedRow directly
    (0 until N).foreach { i =>
      val s = UTF8String.fromString(i.toString)
      assert(i === result.getInt(i + 2))
      assert(s === result.getUTF8String(i + 2 + N))
      assert(i === result.getStruct(0, N * 2).getInt(i))
      assert(s === result.getStruct(0, N * 2).getUTF8String(i + N))
      assert(i === result.getStruct(1, N * 2).getInt(i))
      assert(s === result.getStruct(1, N * 2).getUTF8String(i + N))
    }

    // test generated MutableProjection
    val exprs = nestedSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val mutableProj = GenerateMutableProjection.generate(exprs)
    val row1 = mutableProj(result)
    assert(result === row1)
    val row2 = mutableProj(result)
    assert(result === row2)
  }

  test("generated unsafe projection with array of binary") {
    val row = InternalRow(
      Array[Byte](1, 2),
      new GenericArrayData(Array(Array[Byte](1, 2), null, Array[Byte](3, 4))))
    val fields = (BinaryType :: ArrayType(BinaryType) :: Nil).toArray[DataType]

    val unsafeProj = UnsafeProjection.create(fields)
    val unsafeRow: UnsafeRow = unsafeProj(row)
    assert(java.util.Arrays.equals(unsafeRow.getBinary(0), Array[Byte](1, 2)))
    assert(java.util.Arrays.equals(unsafeRow.getArray(1).getBinary(0), Array[Byte](1, 2)))
    assert(unsafeRow.getArray(1).isNullAt(1))
    assert(unsafeRow.getArray(1).getBinary(1) === null)
    assert(java.util.Arrays.equals(unsafeRow.getArray(1).getBinary(2), Array[Byte](3, 4)))

    val safeProj = SafeProjection.create(fields)
    val row2 = safeProj(unsafeRow)
    assert(row2 === row)
  }

  test("padding bytes should be zeroed out") {
    val types = Seq(BooleanType, ByteType, ShortType, IntegerType, FloatType, BinaryType,
      StringType)
    val struct = StructType(types.map(StructField("", _, true)))
    val fields = Array[DataType](StringType, struct)
    val unsafeProj = UnsafeProjection.create(fields)

    val innerRow = InternalRow(false, 1.toByte, 2.toShort, 3, 4.0f,
      "".getBytes(StandardCharsets.UTF_8),
      UTF8String.fromString(""))
    val row1 = InternalRow(UTF8String.fromString(""), innerRow)
    val unsafe1 = unsafeProj(row1).copy()
    // create a Row with long String before the inner struct
    val row2 = InternalRow(UTF8String.fromString("a_long_string").repeat(10), innerRow)
    val unsafe2 = unsafeProj(row2).copy()
    assert(unsafe1.getStruct(1, 7) === unsafe2.getStruct(1, 7))
    val unsafe3 = unsafeProj(row1).copy()
    assert(unsafe1 === unsafe3)
    assert(unsafe1.getStruct(1, 7) === unsafe3.getStruct(1, 7))
  }

  test("MutableProjection should not cache content from the input row") {
    val mutableProj = GenerateMutableProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))
    val row = new GenericInternalRow(1)
    mutableProj.target(row)

    val unsafeProj = GenerateUnsafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))
    val unsafeRow = unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("a"))))

    mutableProj.apply(unsafeRow)
    assert(row.getStruct(0, 1).getString(0) == "a")

    // Even if the input row of the mutable projection has been changed, the target mutable row
    // should keep same.
    unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("b"))))
    assert(row.getStruct(0, 1).getString(0).toString == "a")
  }

  test("SafeProjection should not cache content from the input row") {
    val safeProj = GenerateSafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))

    val unsafeProj = GenerateUnsafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", StringType), true)))
    val unsafeRow = unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("a"))))

    val row = safeProj.apply(unsafeRow)
    assert(row.getStruct(0, 1).getString(0) == "a")

    // Even if the input row of the mutable projection has been changed, the target mutable row
    // should keep same.
    unsafeProj.apply(InternalRow(InternalRow(UTF8String.fromString("b"))))
    assert(row.getStruct(0, 1).getString(0).toString == "a")
  }

  test("SPARK-22699: GenerateSafeProjection should not use global variables for struct") {
    val safeProj = GenerateSafeProjection.generate(
      Seq(BoundReference(0, new StructType().add("i", IntegerType), true)))
    val globalVariables = safeProj.getClass.getDeclaredFields
    // We need always 3 variables:
    // - one is a reference to this
    // - one is the references object
    // - one is the mutableRow
    assert(globalVariables.length == 3)
  }

  test("SPARK-18016: generated projections on wider table requiring state compaction") {
    val N = 6000
    val wideRow1 = new GenericInternalRow(new Array[Any](N))
    val schema1 = StructType((1 to N).map(i => StructField("", IntegerType)))
    val wideRow2 = new GenericInternalRow(
      Array.tabulate[Any](N)(i => UTF8String.fromString(i.toString)))
    val schema2 = StructType((1 to N).map(i => StructField("", StringType)))
    val joined = new JoinedRow(wideRow1, wideRow2)
    val joinedSchema = StructType(schema1 ++ schema2)
    val nested = new JoinedRow(InternalRow(joined, joined), joined)
    val nestedSchema = StructType(
      Seq(StructField("", joinedSchema), StructField("", joinedSchema)) ++ joinedSchema)

    val safeProj = SafeProjection.create(nestedSchema)
    val result = safeProj(nested)

    // test generated MutableProjection
    val exprs = nestedSchema.fields.zipWithIndex.map { case (f, i) =>
      BoundReference(i, f.dataType, true)
    }
    val mutableProj = GenerateMutableProjection.generate(exprs)
    val row1 = mutableProj(result)
    assert(result === row1)
    val row2 = mutableProj(result)
    assert(result === row2)
  }

  test("SPARK-33473: subexpression elimination for interpreted SafeProjection") {
    Seq("true", "false").foreach { enabled =>
      withSQLConf(
        SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key -> enabled,
        SQLConf.CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString) {
        val one = BoundReference(0, DoubleType, true)
        val two = BoundReference(1, DoubleType, true)

        val mul = Multiply(one, two)
        val mul2 = Multiply(mul, mul)
        val sqrt = Sqrt(mul2)
        val sum = Add(mul2, sqrt)

        val proj = SafeProjection.create(Seq(sum))
        val result = (d1: Double, d2: Double) =>
          ((d1 * d2) * (d1 * d2)) + Math.sqrt((d1 * d2) * (d1 * d2))

        val inputRows = Seq(
          InternalRow.fromSeq(Seq(1.0, 2.0)),
          InternalRow.fromSeq(Seq(2.0, 3.0)),
          InternalRow.fromSeq(Seq(1.0, null)),
          InternalRow.fromSeq(Seq(null, 2.0)),
          InternalRow.fromSeq(Seq(3.0, 4.0)),
          InternalRow.fromSeq(Seq(null, null))
        )
        val expectedResults = Seq(
          result(1.0, 2.0),
          result(2.0, 3.0),
          null,
          null,
          result(3.0, 4.0),
          null
        )

        inputRows.zip(expectedResults).foreach { case (inputRow, expected) =>
          val projRow = proj.apply(inputRow)
          if (expected != null) {
            assert(projRow.getDouble(0) == expected)
          } else {
            assert(projRow.isNullAt(0))
          }
        }
      }
    }
  }

  private[this] def testUnexpectedNullHandling(
      inputType: DataType,
      inputDatum: Any,
      expectedFieldDescriptor: String): Unit = {
    for (topLevelNullable <- Seq(false, true)) {
      val proj = UnsafeProjection.create(BoundReference(0, inputType, nullable = topLevelNullable))
      val e = intercept[RuntimeException](proj(InternalRow(inputDatum)))

      assert(e.getMessage.contains(
        s"The value at $expectedFieldDescriptor cannot be null, but a NULL was found."))
    }
  }

  test("SPARK-40199 Projecting NULLs from non-nullable input should throw a helpful msg") {
    testUnexpectedNullHandling(
      new StructType().add("middle", new StructType().add("bottom", StringType, nullable = false)),
      InternalRow(InternalRow(null)),
      "<POS_0>.`middle`.`bottom`")
  }

  test("SPARK-40199 Projecting NULLs from non-nullable array should throw a helpful msg") {
    testUnexpectedNullHandling(
      ArrayType(StringType, containsNull = false),
      ArrayData.toArrayData(Array(null)),
      "<POS_0>.<ARRAY_ELEMENT>")

    Seq(true, false).foreach { containsNull =>
      testUnexpectedNullHandling(
        ArrayType(new StructType().add("f", StringType, nullable = false), containsNull),
        ArrayData.toArrayData(Array(InternalRow(null))),
        "<POS_0>.<ARRAY_ELEMENT>.`f`")
    }
  }

  test("SPARK-40199 Projecting NULLs from non-nullable map should throw a helpful msg") {
    val foo = UTF8String.fromString("foo")
    def map(key: Any, value: Any): ArrayBasedMapData = ArrayBasedMapData(Array(key), Array(value))

    val mapType = MapType(StringType, StringType, valueContainsNull = false)
    testUnexpectedNullHandling(mapType, map(null, foo), "<POS_0>.<MAP_KEY>")
    testUnexpectedNullHandling(mapType, map(foo, null), "<POS_0>.<MAP_VALUE>")

    val nestType = new StructType().add("f", StringType, nullable = false)
    testUnexpectedNullHandling(
      MapType(nestType, StringType), map(InternalRow(null), foo), "<POS_0>.<MAP_KEY>.`f`")

    Seq(true, false).foreach { containsNull =>
      testUnexpectedNullHandling(
        MapType(StringType, nestType, containsNull),
        map(foo, InternalRow(null)),
        "<POS_0>.<MAP_VALUE>.`f`")
    }
  }

  test("SPARK-40199 Unexpected null handling should handle columns with special chars") {
    val colName = "col_\"'`,.?$!&%@^~+*-\b\t\f\'\r\n+()[]<>{}|\\/_test"
    testUnexpectedNullHandling(
      new StructType().add(colName, StringType, nullable = false),
      InternalRow(null),
      s"<POS_0>.`$colName`")
  }
}
