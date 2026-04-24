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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.{SPARK_DOC_ROOT, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.TypeUtils.ordinalNumber
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

class ComplexTypeSuite extends SparkFunSuite with ExpressionEvalHelper {

  /**
   * Runs through the testFunc for all integral data types.
   *
   * @param testFunc a test function that accepts a conversion function to convert an integer
   *                 into another data type.
   */
  private def testIntegralDataTypes(testFunc: (Int => Any) => Unit): Unit = {
    testFunc(_.toByte)
    testFunc(_.toShort)
    testFunc(identity)
    testFunc(_.toLong)
  }

  test("GetArrayItem") {
    val typeA = ArrayType(StringType)
    val array = Literal.create(Seq("a", "b"), typeA)
    testIntegralDataTypes { convert =>
      checkEvaluation(GetArrayItem(array, Literal(convert(1))), "b")
    }
    val nullArray = Literal.create(null, typeA)
    val nullInt = Literal.create(null, IntegerType)
    checkEvaluation(GetArrayItem(nullArray, Literal(1)), null)
    checkEvaluation(GetArrayItem(array, nullInt), null)
    checkEvaluation(GetArrayItem(nullArray, nullInt), null)

    val nonNullArray = Literal.create(Seq(1), ArrayType(IntegerType, false))
    checkEvaluation(GetArrayItem(nonNullArray, Literal(0)), 1)

    val nestedArray = Literal.create(Seq(Seq(1)), ArrayType(ArrayType(IntegerType)))
    checkEvaluation(GetArrayItem(nestedArray, Literal(0)), Seq(1))
  }

  test("SPARK-33386: GetArrayItem ArrayIndexOutOfBoundsException") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        val array = Literal.create(Seq("a", "b"), ArrayType(StringType))

        if (ansiEnabled) {
          checkExceptionInExpression[Exception](
            GetArrayItem(array, Literal(5)),
            "The index 5 is out of bounds. The array has 2 elements."
          )

          checkExceptionInExpression[Exception](
            GetArrayItem(array, Literal(-1)),
            "The index -1 is out of bounds. The array has 2 elements."
          )
        } else {
          checkEvaluation(GetArrayItem(array, Literal(5)), null)
          checkEvaluation(GetArrayItem(array, Literal(-1)), null)
        }
      }
    }
  }

  test("SPARK-40066: GetMapValue returns null on invalid map value access") {
    Seq(true, false).foreach { ansiEnabled =>
      withSQLConf(SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString) {
        val map = Literal.create(Map(1 -> "a", 2 -> "b"), MapType(IntegerType, StringType))
        checkEvaluation(GetMapValue(map, Literal(5)), null)
      }
    }
  }

  test("SPARK-26637 handles GetArrayItem nullability correctly when input array size is constant") {
    // CreateArray case
    val a = AttributeReference("a", IntegerType, nullable = false)()
    val b = AttributeReference("b", IntegerType, nullable = true)()
    val array = CreateArray(a :: b :: Nil)
    assert(!GetArrayItem(array, Literal(0)).nullable)
    assert(GetArrayItem(array, Literal(1)).nullable)
    assert(!GetArrayItem(array, Subtract(Literal(2), Literal(2))).nullable)
    assert(GetArrayItem(array, AttributeReference("ordinal", IntegerType)()).nullable)

    // GetArrayStructFields case
    val f1 = StructField("a", IntegerType, nullable = false)
    val f2 = StructField("b", IntegerType, nullable = true)
    val structType = StructType(f1 :: f2 :: Nil)
    val c = AttributeReference("c", structType, nullable = false)()
    val inputArray1 = CreateArray(c :: Nil)
    val inputArray1ContainsNull = c.nullable
    val stArray1 = GetArrayStructFields(inputArray1, f1, 0, 2, inputArray1ContainsNull)
    assert(!GetArrayItem(stArray1, Literal(0)).nullable)
    val stArray2 = GetArrayStructFields(inputArray1, f2, 1, 2, inputArray1ContainsNull)
    assert(GetArrayItem(stArray2, Literal(0)).nullable)

    val d = AttributeReference("d", structType, nullable = true)()
    val inputArray2 = CreateArray(c :: d :: Nil)
    val inputArray2ContainsNull = c.nullable || d.nullable
    val stArray3 = GetArrayStructFields(inputArray2, f1, 0, 2, inputArray2ContainsNull)
    assert(!GetArrayItem(stArray3, Literal(0)).nullable)
    assert(GetArrayItem(stArray3, Literal(1)).nullable)
    val stArray4 = GetArrayStructFields(inputArray2, f2, 1, 2, inputArray2ContainsNull)
    assert(GetArrayItem(stArray4, Literal(0)).nullable)
    assert(GetArrayItem(stArray4, Literal(1)).nullable)
  }

  Seq((Int.MaxValue, "Linear Lookup"), (0, "Hash Lookup")).foreach { case (threshold, name) =>
    test(s"GetMapValue - $name") {
      withSQLConf(SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> threshold.toString) {
        val typeM = MapType(StringType, StringType)
        val map = Literal.create(Map("a" -> "b"), typeM)
        val nullMap = Literal.create(null, typeM)
        val nullString = Literal.create(null, StringType)

        // 1. Basic lookup (String keys)
        checkEvaluation(GetMapValue(map, Literal("a")), "b")
        checkEvaluation(GetMapValue(map, nullString), null)
        checkEvaluation(GetMapValue(nullMap, nullString), null)
        checkEvaluation(GetMapValue(map, nullString), null)

        val nonNullMap = Literal.create(Map("a" -> 1), MapType(StringType, IntegerType, false))
        checkEvaluation(GetMapValue(nonNullMap, Literal("a")), 1)

        // 2. Nested map
        val nestedMap = Literal.create(Map("a" -> Map("b" -> "c")), MapType(StringType, typeM))
        checkEvaluation(GetMapValue(nestedMap, Literal("a")), Map("b" -> "c"))

        // 3. Basic lookup (Int keys)
        val intMap = Literal.create(Map(1 -> 10, 2 -> 20, 3 -> 30),
          MapType(IntegerType, IntegerType))
        checkEvaluation(GetMapValue(intMap, Literal(1)), 10)
        checkEvaluation(GetMapValue(intMap, Literal(2)), 20)
        checkEvaluation(GetMapValue(intMap, Literal(3)), 30)
        checkEvaluation(GetMapValue(intMap, Literal(4)), null)

        val emptyMap = Literal.create(Map.empty[Int, Int], MapType(IntegerType, IntegerType))
        checkEvaluation(GetMapValue(emptyMap, Literal(1)), null)

        // 4. Special data
        // Duplicate keys: Spark MapType doesn't enforce uniqueness in the underlying
        // data structure (ArrayBasedMapData)
        // We construct it manually to simulate duplicates.
        val keys = new GenericArrayData(Array(1, 2, 1))
        val values = new GenericArrayData(Array(10, 20, 30))
        val dupMapData = new ArrayBasedMapData(keys, values)
        val dupMap = Literal.create(dupMapData, MapType(IntegerType, IntegerType))
        // Should return the first match
        checkEvaluation(GetMapValue(dupMap, Literal(1)), 10)
        checkEvaluation(GetMapValue(dupMap, Literal(2)), 20)

        // Null values
        val nullValueMap = Literal.create(Map(1 -> null), MapType(IntegerType, StringType))
        checkEvaluation(GetMapValue(nullValueMap, Literal(1)), null)

        // NaN keys
        val nan = Double.NaN
        val floatNan = Float.NaN
        val doubleMap = Literal.create(Map(1.0 -> 10, nan -> 20), MapType(DoubleType, IntegerType))
        checkEvaluation(GetMapValue(doubleMap, Literal(1.0)), 10)
        checkEvaluation(GetMapValue(doubleMap, Literal(nan)), 20)

        val floatMap = Literal.create(Map(1.0f -> 10, floatNan -> 20),
          MapType(FloatType, IntegerType))
        checkEvaluation(GetMapValue(floatMap, Literal(1.0f)), 10)
        checkEvaluation(GetMapValue(floatMap, Literal(floatNan)), 20)

        // 5. Key types
        // Long
        val longMap = Literal.create(Map(1L -> 10, 2L -> 20), MapType(LongType, IntegerType))
        checkEvaluation(GetMapValue(longMap, Literal(1L)), 10)
        checkEvaluation(GetMapValue(longMap, Literal(3L)), null)

        // String
        val stringMap = Literal.create(Map("a" -> "A", "b" -> "B"), MapType(StringType, StringType))
        checkEvaluation(GetMapValue(stringMap, Literal("a")), "A")
        checkEvaluation(GetMapValue(stringMap, Literal("c")), null)

        // Decimal
        val decType = DecimalType(10, 2)
        val d1 = Decimal("1.00")
        val d2 = Decimal("2.50")
        val d3 = Decimal("3.75")
        val decimalMap = Literal.create(
          Map(d1 -> 10, d2 -> 20, d3 -> 30),
          MapType(decType, IntegerType))
        checkEvaluation(GetMapValue(decimalMap, Literal(d1, decType)), 10)
        checkEvaluation(GetMapValue(decimalMap, Literal(d2, decType)), 20)
        checkEvaluation(GetMapValue(decimalMap, Literal(d3, decType)), 30)
        checkEvaluation(GetMapValue(decimalMap, Literal(Decimal("9.99"), decType)), null)

        // Decimal with different precision/scale
        val decType2 = DecimalType(38, 18)
        val d4 = Decimal("12345678901234567890.123456789012345678")
        val d5 = Decimal("99999999999999999999.999999999999999999")
        val bigDecimalMap = Literal.create(
          Map(d4 -> 100, d5 -> 200),
          MapType(decType2, IntegerType))
        checkEvaluation(GetMapValue(bigDecimalMap, Literal(d4, decType2)), 100)
        checkEvaluation(GetMapValue(bigDecimalMap, Literal(d5, decType2)), 200)
        checkEvaluation(GetMapValue(bigDecimalMap,
          Literal(Decimal("0.000000000000000001"), decType2)), null)

        // Decimal zero key
        val zeroDecMap = Literal.create(
          Map(Decimal("0.00") -> 99, d1 -> 10),
          MapType(decType, IntegerType))
        checkEvaluation(GetMapValue(zeroDecMap, Literal(Decimal("0.00"), decType)), 99)
        checkEvaluation(GetMapValue(zeroDecMap, Literal(d1, decType)), 10)

        // Decimal negative keys
        val negDecMap = Literal.create(
          Map(Decimal("-1.50") -> 10, Decimal("-99.99") -> 20, d1 -> 30),
          MapType(decType, IntegerType))
        checkEvaluation(GetMapValue(negDecMap, Literal(Decimal("-1.50"), decType)), 10)
        checkEvaluation(GetMapValue(negDecMap, Literal(Decimal("-99.99"), decType)), 20)
        checkEvaluation(GetMapValue(negDecMap, Literal(d1, decType)), 30)
        checkEvaluation(GetMapValue(negDecMap, Literal(Decimal("1.50"), decType)), null)

        // Decimal key with null value
        val decNullValMap = Literal.create(
          Map(d1 -> null, d2 -> 20),
          MapType(decType, IntegerType))
        checkEvaluation(GetMapValue(decNullValMap, Literal(d1, decType)), null)
        checkEvaluation(GetMapValue(decNullValMap, Literal(d2, decType)), 20)

        // Decimal at compact/BigDecimal boundary (precision=18)
        val decType18 = DecimalType(18, 0)
        val boundary1 = Decimal(999999999999999999L)
        val boundary2 = Decimal(-999999999999999999L)
        val boundaryMap = Literal.create(
          Map(boundary1 -> 100, boundary2 -> 200),
          MapType(decType18, IntegerType))
        checkEvaluation(GetMapValue(boundaryMap, Literal(boundary1, decType18)), 100)
        checkEvaluation(GetMapValue(boundaryMap, Literal(boundary2, decType18)), 200)
        checkEvaluation(GetMapValue(boundaryMap, Literal(Decimal(0L), decType18)), null)

        // Decimal duplicate keys
        val decKeys = new GenericArrayData(Array(d1, d2, d1))
        val decValues = new GenericArrayData(Array(10, 20, 30))
        val dupDecMapData = new ArrayBasedMapData(decKeys, decValues)
        val dupDecMap = Literal.create(dupDecMapData, MapType(decType, IntegerType))
        checkEvaluation(GetMapValue(dupDecMap, Literal(d1, decType)), 10)
        checkEvaluation(GetMapValue(dupDecMap, Literal(d2, decType)), 20)

        // Time keys
        val timeType = TimeType()
        val t1 = DateTimeTestUtils.localTime(10, 30, 0)
        val t2 = DateTimeTestUtils.localTime(14, 0, 30)
        val t3 = DateTimeTestUtils.localTime(23, 59, 59, 999999)
        val timeMap = Literal.create(Map(t1 -> 10, t2 -> 20, t3 -> 30),
          MapType(timeType, IntegerType))
        checkEvaluation(GetMapValue(timeMap, Literal(t1, timeType)), 10)
        checkEvaluation(GetMapValue(timeMap, Literal(t2, timeType)), 20)
        checkEvaluation(GetMapValue(timeMap, Literal(t3, timeType)), 30)
        checkEvaluation(GetMapValue(timeMap,
          Literal(DateTimeTestUtils.localTime(0, 0, 0), timeType)), null)

        // Time zero (midnight)
        val t0 = DateTimeTestUtils.localTime()
        val timeZeroMap = Literal.create(Map(t0 -> 99, t1 -> 10),
          MapType(timeType, IntegerType))
        checkEvaluation(GetMapValue(timeZeroMap, Literal(t0, timeType)), 99)

        // Time duplicate keys
        val timeKeys = new GenericArrayData(Array(t1, t2, t1))
        val timeValues = new GenericArrayData(Array(10, 20, 30))
        val dupTimeMapData = new ArrayBasedMapData(timeKeys, timeValues)
        val dupTimeMap = Literal.create(dupTimeMapData, MapType(timeType, IntegerType))
        checkEvaluation(GetMapValue(dupTimeMap, Literal(t1, timeType)), 10)
        checkEvaluation(GetMapValue(dupTimeMap, Literal(t2, timeType)), 20)

        // 6. Binary Keys
        val binaryMap = Literal.create(Map(Array(1.toByte) -> 10, Array(2.toByte) -> 20),
          MapType(BinaryType, IntegerType))
        checkEvaluation(GetMapValue(binaryMap, Literal(Array(1.toByte))), 10)
        checkEvaluation(GetMapValue(binaryMap, Literal(Array(3.toByte))), null)

        // 7. Array Keys
        val arrayType = ArrayType(IntegerType)
        val arrayMap = Literal.create(
          Map(Array(1, 2) -> 10, Array(3, 4) -> 20),
          MapType(arrayType, IntegerType))
        checkEvaluation(GetMapValue(arrayMap, Literal.create(Array(1, 2), arrayType)), 10)
        checkEvaluation(GetMapValue(arrayMap, Literal.create(Array(3, 4), arrayType)), 20)
        checkEvaluation(GetMapValue(arrayMap, Literal.create(Array(5, 6), arrayType)), null)

        // 8. Struct Keys
        val structType = new StructType().add("a", "int").add("b", "int")
        val structMap = Literal.create(
          Map(create_row(1, 1) -> 10, create_row(2, 2) -> 20),
          MapType(structType, IntegerType))
        checkEvaluation(GetMapValue(structMap, Literal.create(create_row(1, 1), structType)), 10)
        checkEvaluation(GetMapValue(structMap, Literal.create(create_row(2, 2), structType)), 20)
        checkEvaluation(GetMapValue(structMap, Literal.create(create_row(3, 3), structType)), null)
      }
    }
  }

  test("GetMapValue - non-foldable map always uses linear scan") {
    // With threshold=0, a foldable map would take the hash path. A non-foldable map (backed by
    // a row column) must still fall back to linear scan, because its hash index cannot be
    // reused across rows (building it per row is a perf regression vs. linear).
    withSQLConf(SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> "0") {
      val mapType = MapType(IntegerType, IntegerType)
      val mapData = ArrayBasedMapData(Map(1 -> 10, 2 -> 20, 3 -> 30))
      val row = create_row(mapData)
      val mapRef = BoundReference(0, mapType, nullable = true)
      assert(!mapRef.foldable)

      // Behavior still correct.
      checkEvaluation(GetMapValue(mapRef, Literal(1)), 10, row)
      checkEvaluation(GetMapValue(mapRef, Literal(2)), 20, row)
      checkEvaluation(GetMapValue(mapRef, Literal(4)), null, row)

      checkEvaluation(ElementAt(mapRef, Literal(3)), 30, row)
      checkEvaluation(ElementAt(mapRef, Literal(99)), null, row)

      // A null map from a non-foldable reference must still return null.
      val nullRow = create_row(null)
      checkEvaluation(GetMapValue(mapRef, Literal(1)), null, nullRow)

      // Strategy choice: non-foldable input never takes the hash path, independent of
      // threshold. This guards against future refactors that accidentally route every map
      // through the hash executor (a regression that behavior-only tests wouldn't catch).
      assert(!GetMapValue(mapRef, Literal(1)).usesFoldableHashLookup)
      assert(!ElementAt(mapRef, Literal(1)).usesFoldableHashLookup)
    }
  }

  test("GetMapValue - strategy choice for foldable maps") {
    // Build a foldable map literal large enough to clear the default threshold. The
    // strategy assertions here pair with the non-foldable test above: together they lock in
    // that foldability, not size alone, gates the hash path.
    val entries = (0 until 2000).map(i => i -> i.toString).toMap
    val foldableLit = Literal.create(entries, MapType(IntegerType, StringType))
    val smallLit = Literal.create(Map(1 -> "a"), MapType(IntegerType, StringType))
    val binaryKeyLit = Literal.create(
      Map(Array[Byte](1) -> 10), MapType(BinaryType, IntegerType))

    // Foldable + above threshold + hashable key type --> PrebuiltHashExecutor.
    withSQLConf(SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> "1000") {
      assert(GetMapValue(foldableLit, Literal(1)).usesFoldableHashLookup)
      assert(ElementAt(foldableLit, Literal(1)).usesFoldableHashLookup)
    }

    // Foldable but below threshold --> LinearExecutor.
    withSQLConf(SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> "10000") {
      assert(!GetMapValue(foldableLit, Literal(1)).usesFoldableHashLookup)
    }

    // Foldable, size=1, threshold=0 --> still LinearExecutor (threshold is >=, len < threshold
    // is false here but we want to also confirm small maps don't regress with threshold=0).
    withSQLConf(SQLConf.MAP_LOOKUP_HASH_THRESHOLD.key -> "0") {
      assert(GetMapValue(smallLit, Literal(1)).usesFoldableHashLookup)

      // Unsupported key type (BinaryType fails typeWithProperEquals) --> LinearExecutor even
      // when foldable and threshold=0.
      assert(!GetMapValue(binaryKeyLit, Literal(Array[Byte](1))).usesFoldableHashLookup)
    }
  }

  test("GetStructField") {
    val typeS = StructType(StructField("a", IntegerType) :: Nil)
    val struct = Literal.create(create_row(1), typeS)
    val nullStruct = Literal.create(null, typeS)

    def getStructField(expr: Expression, fieldName: String): GetStructField = {
      expr.dataType match {
        case StructType(fields) =>
          val index = fields.indexWhere(_.name == fieldName)
          GetStructField(expr, index)
      }
    }

    checkEvaluation(getStructField(struct, "a"), 1)
    checkEvaluation(getStructField(nullStruct, "a"), null)

    val nestedStruct = Literal.create(create_row(create_row(1)),
      StructType(StructField("a", typeS) :: Nil))
    checkEvaluation(getStructField(nestedStruct, "a"), create_row(1))

    val typeS_fieldNotNullable = StructType(StructField("a", IntegerType, false) :: Nil)
    val struct_fieldNotNullable = Literal.create(create_row(1), typeS_fieldNotNullable)
    val nullStruct_fieldNotNullable = Literal.create(null, typeS_fieldNotNullable)

    assert(getStructField(struct_fieldNotNullable, "a").nullable === false)
    assert(getStructField(struct, "a").nullable)
    assert(getStructField(nullStruct_fieldNotNullable, "a").nullable)
    assert(getStructField(nullStruct, "a").nullable)
  }

  test("GetStructField checkInputDataTypes should fail when child is not StructType") {
    // Simulate a plan transformation that changes the child's type from StructType to
    // StringType after the GetStructField was created.
    val stringAttr = AttributeReference("c2", StringType)()
    val getField = GetStructField(stringAttr, 0, Some("f1"))

    assert(getField.checkInputDataTypes().isFailure)
    val result = getField.checkInputDataTypes().asInstanceOf[DataTypeMismatch]
    assert(result.errorSubClass == "UNEXPECTED_INPUT_TYPE")
  }

  test("GetStructField toPrettySQL should not crash when child is not StructType") {
    val stringAttr = AttributeReference("c2", StringType)()
    val getField = GetStructField(stringAttr, 0, Some("f1"))

    // This should not throw ClassCastException
    val prettySQL = toPrettySQL(getField)
    assert(prettySQL.contains("c2"))
    assert(prettySQL.contains("f1"))
  }

  test("GetArrayStructFields") {
    // test 4 types: struct field nullability X array element nullability
    val type1 = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val type2 = ArrayType(StructType(StructField("a", IntegerType, nullable = false) :: Nil))
    val type3 = ArrayType(StructType(StructField("a", IntegerType) :: Nil), containsNull = false)
    val type4 = ArrayType(
      StructType(StructField("a", IntegerType, nullable = false) :: Nil), containsNull = false)

    val input1 = Literal.create(Seq(create_row(1)), type4)
    val input2 = Literal.create(Seq(create_row(null)), type3)
    val input3 = Literal.create(Seq(null), type2)
    val input4 = Literal.create(null, type1)

    def getArrayStructFields(expr: Expression, fieldName: String): Expression = {
      ExtractValue.apply(expr, Literal.create(fieldName, StringType), _ == _)
    }

    checkEvaluation(getArrayStructFields(input1, "a"), Seq(1))
    checkEvaluation(getArrayStructFields(input2, "a"), Seq(null))
    checkEvaluation(getArrayStructFields(input3, "a"), Seq(null))
    checkEvaluation(getArrayStructFields(input4, "a"), null)
  }

  test("SPARK-32167: nullability of GetArrayStructFields") {
    val resolver = SQLConf.get.resolver

    val array1 = ArrayType(
      new StructType().add("a", "int", nullable = true),
      containsNull = false)
    val data1 = Literal.create(Seq(Row(null)), array1)
    val get1 = ExtractValue(data1, Literal("a"), resolver).asInstanceOf[GetArrayStructFields]
    assert(get1.containsNull)

    val array2 = ArrayType(
      new StructType().add("a", "int", nullable = false),
      containsNull = true)
    val data2 = Literal.create(Seq(null), array2)
    val get2 = ExtractValue(data2, Literal("a"), resolver).asInstanceOf[GetArrayStructFields]
    assert(get2.containsNull)

    val array3 = ArrayType(
      new StructType().add("a", "int", nullable = false),
      containsNull = false)
    val data3 = Literal.create(Seq(Row(1)), array3)
    val get3 = ExtractValue(data3, Literal("a"), resolver).asInstanceOf[GetArrayStructFields]
    assert(!get3.containsNull)
  }

  test("CreateArray") {
    val intSeq = Seq(5, 10, 15, 20, 25)
    val longSeq = intSeq.map(_.toLong)
    val byteSeq = intSeq.map(_.toByte)
    val strSeq = intSeq.map(_.toString)
    checkEvaluation(CreateArray(intSeq.map(Literal(_))), intSeq, EmptyRow)
    checkEvaluation(CreateArray(longSeq.map(Literal(_))), longSeq, EmptyRow)
    checkEvaluation(CreateArray(byteSeq.map(Literal(_))), byteSeq, EmptyRow)
    checkEvaluation(CreateArray(strSeq.map(Literal(_))), strSeq, EmptyRow)

    val intWithNull = intSeq.map(Literal(_)) :+ Literal.create(null, IntegerType)
    val longWithNull = longSeq.map(Literal(_)) :+ Literal.create(null, LongType)
    val byteWithNull = byteSeq.map(Literal(_)) :+ Literal.create(null, ByteType)
    val strWithNull = strSeq.map(Literal(_)) :+ Literal.create(null, StringType)
    checkEvaluation(CreateArray(intWithNull), intSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(longWithNull), longSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(byteWithNull), byteSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(strWithNull), strSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(Literal.create(null, IntegerType) :: Nil), null :: Nil)

    val array = CreateArray(Seq(
      Literal.create(intSeq, ArrayType(IntegerType, containsNull = false)),
      Literal.create(intSeq :+ null, ArrayType(IntegerType, containsNull = true))))
    assert(array.dataType ===
      ArrayType(ArrayType(IntegerType, containsNull = true), containsNull = false))
    checkEvaluation(array, Seq(intSeq, intSeq :+ null))
  }

  test("CreateMap") {
    def interlace(keys: Seq[Literal], values: Seq[Literal]): Seq[Literal] = {
      keys.zip(values).flatMap { case (k, v) => Seq(k, v) }
    }

    val intSeq = Seq(5, 10, 15, 20, 25)
    val longSeq = intSeq.map(_.toLong)
    val strSeq = intSeq.map(_.toString)

    checkEvaluation(CreateMap(Nil), Map.empty)
    checkEvaluation(
      CreateMap(interlace(intSeq.map(Literal(_)), longSeq.map(Literal(_)))),
      create_map(intSeq, longSeq))
    checkEvaluation(
      CreateMap(interlace(strSeq.map(Literal(_)), longSeq.map(Literal(_)))),
      create_map(strSeq, longSeq))
    checkEvaluation(
      CreateMap(interlace(longSeq.map(Literal(_)), strSeq.map(Literal(_)))),
      create_map(longSeq, strSeq))

    val strWithNull = strSeq.drop(1).map(Literal(_)) :+ Literal.create(null, StringType)
    checkEvaluation(
      CreateMap(interlace(intSeq.map(Literal(_)), strWithNull)),
      create_map(intSeq, strWithNull.map(_.value)))

    // Map key can't be null
    checkErrorInExpression[SparkRuntimeException](
      CreateMap(interlace(strWithNull, intSeq.map(Literal(_)))),
      "NULL_MAP_KEY")

    checkErrorInExpression[SparkRuntimeException](
      CreateMap(Seq(Literal(1), Literal(2), Literal(1), Literal(3))),
      condition = "DUPLICATED_MAP_KEY",
      parameters = Map(
        "key" -> "1",
        "mapKeyDedupPolicy" -> "\"spark.sql.mapKeyDedupPolicy\"")
    )
    withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
      // Duplicated map keys will be removed w.r.t. the last wins policy.
      checkEvaluation(
        CreateMap(Seq(Literal(1), Literal(2), Literal(1), Literal(3))),
        create_map(1 -> 3))
    }

    // ArrayType map key and value
    val map = CreateMap(Seq(
      Literal.create(intSeq, ArrayType(IntegerType, containsNull = false)),
      Literal.create(strSeq, ArrayType(StringType, containsNull = false)),
      Literal.create(intSeq :+ null, ArrayType(IntegerType, containsNull = true)),
      Literal.create(strSeq :+ null, ArrayType(StringType, containsNull = true))))
    assert(map.dataType ===
      MapType(
        ArrayType(IntegerType, containsNull = true),
        ArrayType(StringType, containsNull = true),
        valueContainsNull = false))
    checkEvaluation(map, create_map(intSeq -> strSeq, (intSeq :+ null) -> (strSeq :+ null)))

    // map key can't be map
    val map2 = CreateMap(Seq(
      Literal.create(create_map(1 -> 1), MapType(IntegerType, IntegerType)),
      Literal(1)
    ))
    map2.checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckSuccess => fail("should not allow map as map key")
      case TypeCheckResult.DataTypeMismatch(errorSubClass, messageParameters) =>
        assert(errorSubClass == "INVALID_MAP_KEY_TYPE")
        assert(messageParameters === Map("keyType" -> "\"MAP<INT, INT>\""))
    }

    // expects a positive even number of arguments
    val map3 = CreateMap(Seq(Literal(1), Literal(2), Literal(3)))
    checkError(
      exception = intercept[AnalysisException] {
        map3.checkInputDataTypes()
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`map`",
        "expectedNum" -> "2n (n > 0)",
        "actualNum" -> "3",
        "docroot" -> SPARK_DOC_ROOT)
    )

    // The given keys of function map should all be the same type
    val map4 = CreateMap(Seq(Literal(1), Literal(2), Literal('a'), Literal(3)))
    assert(map4.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "CREATE_MAP_KEY_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> "`map`",
          "dataType" -> "[\"INT\", \"STRING\"]")
      )
    )

    // The given values of function map should all be the same type
    val map5 = CreateMap(Seq(Literal(1), Literal(2), Literal(3), Literal('a')))
    assert(map5.checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "CREATE_MAP_VALUE_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> "`map`",
          "dataType" -> "[\"INT\", \"STRING\"]")
      )
    )
  }

  // map key can't be variant
  val map6 = CreateMap(Seq(
    Literal.create(new VariantVal(Array[Byte](), Array[Byte]())),
    Literal.create(1)
  ))
  map6.checkInputDataTypes() match {
    case TypeCheckResult.TypeCheckSuccess => fail("should not allow variant as a part of map key")
    case TypeCheckResult.DataTypeMismatch(errorSubClass, messageParameters) =>
      assert(errorSubClass == "INVALID_MAP_KEY_TYPE")
      assert(messageParameters === Map("keyType" -> "\"VARIANT\""))
  }

  // map key can't contain variant
  val map7 = CreateMap(
    Seq(
      CreateStruct(
        Seq(Literal.create(1), Literal.create(new VariantVal(Array[Byte](), Array[Byte]())))
      ),
      Literal.create(1)
    )
  )
  map7.checkInputDataTypes() match {
    case TypeCheckResult.TypeCheckSuccess => fail("should not allow variant as a part of map key")
    case TypeCheckResult.DataTypeMismatch(errorSubClass, messageParameters) =>
      assert(errorSubClass == "INVALID_MAP_KEY_TYPE")
      assert(
        messageParameters === Map(
          "keyType" -> "\"STRUCT<col1: INT NOT NULL, col2: VARIANT NOT NULL>\""
        )
      )
  }

  test("MapFromArrays") {
    val intSeq = Seq(5, 10, 15, 20, 25)
    val longSeq = intSeq.map(_.toLong)
    val strSeq = intSeq.map(_.toString)
    val integerSeq = Seq[java.lang.Integer](5, 10, 15, 20, 25)
    val intWithNullSeq = Seq[java.lang.Integer](5, 10, null, 20, 25)
    val longWithNullSeq = intSeq.map(java.lang.Long.valueOf(_))

    val intArray = Literal.create(intSeq, ArrayType(IntegerType, false))
    val longArray = Literal.create(longSeq, ArrayType(LongType, false))
    val strArray = Literal.create(strSeq, ArrayType(StringType, false))

    val integerArray = Literal.create(integerSeq, ArrayType(IntegerType, true))
    val intWithNullArray = Literal.create(intWithNullSeq, ArrayType(IntegerType, true))
    val longWithNullArray = Literal.create(longWithNullSeq, ArrayType(LongType, true))

    val nullArray = Literal.create(null, ArrayType(StringType, false))

    checkEvaluation(MapFromArrays(intArray, longArray), create_map(intSeq, longSeq))
    checkEvaluation(MapFromArrays(intArray, strArray), create_map(intSeq, strSeq))
    checkEvaluation(MapFromArrays(integerArray, strArray), create_map(integerSeq, strSeq))

    checkEvaluation(
      MapFromArrays(strArray, intWithNullArray), create_map(strSeq, intWithNullSeq))
    checkEvaluation(
      MapFromArrays(strArray, longWithNullArray), create_map(strSeq, longWithNullSeq))
    checkEvaluation(
      MapFromArrays(strArray, longWithNullArray), create_map(strSeq, longWithNullSeq))
    checkEvaluation(MapFromArrays(nullArray, nullArray), null)

    // Map key can't be null
    checkErrorInExpression[SparkRuntimeException](
      MapFromArrays(intWithNullArray, strArray),
      "NULL_MAP_KEY")

    checkErrorInExpression[SparkRuntimeException](
      MapFromArrays(
        Literal.create(Seq(1, 1), ArrayType(IntegerType)),
        Literal.create(Seq(2, 3), ArrayType(IntegerType))),
      condition = "DUPLICATED_MAP_KEY",
      parameters = Map(
        "key" -> "1",
        "mapKeyDedupPolicy" -> "\"spark.sql.mapKeyDedupPolicy\"")
    )
    withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
      // Duplicated map keys will be removed w.r.t. the last wins policy.
      checkEvaluation(
        MapFromArrays(
          Literal.create(Seq(1, 1), ArrayType(IntegerType)),
          Literal.create(Seq(2, 3), ArrayType(IntegerType))),
        create_map(1 -> 3))
    }

    // map key can't be map
    val arrayOfMap = Seq(create_map(1 -> "a", 2 -> "b"))
    val map = MapFromArrays(
      Literal.create(arrayOfMap, ArrayType(MapType(IntegerType, StringType))),
      Literal.create(Seq(1), ArrayType(IntegerType)))
    map.checkInputDataTypes() match {
      case TypeCheckResult.TypeCheckSuccess => fail("should not allow map as map key")
      case TypeCheckResult.DataTypeMismatch(errorSubClass, messageParameters) =>
        assert(errorSubClass == "INVALID_MAP_KEY_TYPE")
        assert(messageParameters === Map("keyType" -> "\"MAP<INT, STRING>\""))
    }
  }

  test("CreateStruct") {
    val row = create_row(1, 2, 3)
    val c1 = $"a".int.at(0)
    val c3 = $"c".int.at(2)
    checkEvaluation(CreateStruct(Seq(c1, c3)), create_row(1, 3), row)
    checkEvaluation(CreateStruct(Literal.create(null, LongType) :: Nil), create_row(null))
  }

  test("CreateNamedStruct") {
    val row = create_row(1, 2, 3)
    val c1 = $"a".int.at(0)
    val c3 = $"c".int.at(2)
    checkEvaluation(CreateNamedStruct(Seq("a", c1, "b", c3)), create_row(1, 3), row)
    checkEvaluation(CreateNamedStruct(Seq("a", c1, "b", "y")),
      create_row(1, UTF8String.fromString("y")), row)
    checkEvaluation(CreateNamedStruct(Seq("a", "x", "b", 2.0)),
      create_row(UTF8String.fromString("x"), 2.0))
    checkEvaluation(CreateNamedStruct(Seq("a", Literal.create(null, IntegerType))),
      create_row(null))

    // expects a positive even number of arguments
    val namedStruct1 = CreateNamedStruct(Seq(Literal(1), Literal(2), Literal(3)))
    checkError(
      exception = intercept[AnalysisException] {
        namedStruct1.checkInputDataTypes()
      },
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map(
        "functionName" -> "`named_struct`",
        "expectedNum" -> "2n (n > 0)",
        "actualNum" -> "3",
        "docroot" -> SPARK_DOC_ROOT)
    )
  }

  test("test dsl for complex type") {
    def quickResolve(u: UnresolvedExtractValue): Expression = {
      ExtractValue(u.child, u.extraction, _ == _)
    }

    checkEvaluation(quickResolve(Symbol("c")
      .map(MapType(StringType, StringType)).at(0).getItem("a")),
      "b", create_row(Map("a" -> "b")))
    checkEvaluation(quickResolve($"c".array(StringType).at(0).getItem(1)),
      "b", create_row(Seq("a", "b")))
    checkEvaluation(quickResolve($"c".struct($"a".int).at(0).getField("a")),
      1, create_row(create_row(1)))
  }

  test("ensure to preserve metadata") {
    val metadata = new MetadataBuilder()
      .putString("key", "value")
      .build()

    def checkMetadata(expr: Expression): Unit = {
      assert(expr.dataType.asInstanceOf[StructType]("a").metadata === metadata)
      assert(expr.dataType.asInstanceOf[StructType]("b").metadata === Metadata.empty)
    }

    val a = AttributeReference("a", IntegerType, metadata = metadata)()
    val b = AttributeReference("b", IntegerType)()
    checkMetadata(CreateStruct(Seq(a, b)))
    checkMetadata(CreateNamedStruct(Seq("a", a, "b", b)))
  }

  test("StringToMap") {
    val expectedDataType = MapType(StringType, StringType, valueContainsNull = true)
    assert(new StringToMap("").dataType === expectedDataType)

    val s0 = Literal("a:1,b:2,c:3")
    val m0 = Map("a" -> "1", "b" -> "2", "c" -> "3")
    checkEvaluation(new StringToMap(s0), m0)

    val s1 = Literal("a: ,b:2")
    val m1 = Map("a" -> " ", "b" -> "2")
    checkEvaluation(new StringToMap(s1), m1)

    val s2 = Literal("a=1,b=2,c=3")
    val m2 = Map("a" -> "1", "b" -> "2", "c" -> "3")
    checkEvaluation(StringToMap(s2, Literal(","), Literal("=")), m2)

    val s3 = Literal("")
    val m3 = Map[String, String]("" -> null)
    checkEvaluation(StringToMap(s3, Literal(","), Literal("=")), m3)

    val s4 = Literal("a:1_b:2_c:3")
    val m4 = Map("a" -> "1", "b" -> "2", "c" -> "3")
    checkEvaluation(new StringToMap(s4, Literal("_")), m4)

    val s5 = Literal("a")
    val m5 = Map("a" -> null)
    checkEvaluation(new StringToMap(s5), m5)

    val s6 = Literal("a=1&b=2&c=3")
    val m6 = Map("a" -> "1", "b" -> "2", "c" -> "3")
    checkEvaluation(StringToMap(s6, NonFoldableLiteral("&"), NonFoldableLiteral("=")), m6)

    checkErrorInExpression[SparkRuntimeException](
      new StringToMap(Literal("a:1,b:2,a:3")),
      condition = "DUPLICATED_MAP_KEY",
      parameters = Map(
        "key" -> "a",
        "mapKeyDedupPolicy" -> "\"spark.sql.mapKeyDedupPolicy\"")
    )
    withSQLConf(SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
      // Duplicated map keys will be removed w.r.t. the last wins policy.
      checkEvaluation(
        new StringToMap(Literal("a:1,b:2,a:3")),
        create_map("a" -> "3", "b" -> "2"))
    }

    // arguments checking
    assert(new StringToMap(Literal("a:1,b:2,c:3")).checkInputDataTypes().isSuccess)
    assert(new StringToMap(Literal(null)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"STRING\"",
          "inputSql" -> "\"NULL\"",
          "inputType" -> "\"VOID\""
        )
      )
    )
    assert(new StringToMap(Literal("a:1,b:2,c:3"), Literal(null)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"STRING\"",
          "inputSql" -> "\"NULL\"",
          "inputType" -> "\"VOID\""
        )
      )
    )
    assert(StringToMap(Literal("a:1,b:2,c:3"), Literal(null),
      Literal(null)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(1),
          "requiredType" -> "\"STRING\"",
          "inputSql" -> "\"NULL\"",
          "inputType" -> "\"VOID\""
        )
      )
    )
    assert(new StringToMap(Literal(null), Literal(null)).checkInputDataTypes() ==
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> "\"STRING\"",
          "inputSql" -> "\"NULL\"",
          "inputType" -> "\"VOID\""
        )
      )
    )
  }

  test("SPARK-22693: CreateNamedStruct should not use global variables") {
    val ctx = new CodegenContext
    CreateNamedStruct(Seq("a", "x", "b", 2.0)).genCode(ctx)
    assert(ctx.inlinedMutableStates.isEmpty)
  }

  test("SPARK-33338: semanticEquals should handle static GetMapValue correctly") {
    val keys = new Array[UTF8String](1)
    val values = new Array[UTF8String](1)
    keys(0) = UTF8String.fromString("key")
    values(0) = UTF8String.fromString("value")

    val d1 = new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    val d2 = new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    val m1 = GetMapValue(Literal.create(d1, MapType(StringType, StringType)), Literal("a"))
    val m2 = GetMapValue(Literal.create(d2, MapType(StringType, StringType)), Literal("a"))

    assert(m1.semanticEquals(m2))
  }

  test("SPARK-40315: Literals of ArrayBasedMapData should have deterministic hashCode.") {
    val keys = new Array[UTF8String](1)
    val values1 = new Array[UTF8String](1)
    val values2 = new Array[UTF8String](1)

    keys(0) = UTF8String.fromString("key")
    values1(0) = UTF8String.fromString("value1")
    values2(0) = UTF8String.fromString("value2")

    val d1 = new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values1))
    val d2 = new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values1))
    val d3 = new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values2))
    val m1 = Literal.create(d1, MapType(StringType, StringType))
    val m2 = Literal.create(d2, MapType(StringType, StringType))
    val m3 = Literal.create(d3, MapType(StringType, StringType))

    // If two Literals of ArrayBasedMapData have the same elements, we expect them to be equal and
    // to have the same hashCode().
    assert(m1 == m2)
    assert(m1.hashCode() == m2.hashCode())
    // If two Literals of ArrayBasedMapData have different elements, we expect them not to be equal
    // and to have different hashCode().
    assert(m1 != m3)
    assert(m1.hashCode() != m3.hashCode())
  }
}
