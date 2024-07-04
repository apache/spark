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

  test("GetMapValue") {
    val typeM = MapType(StringType, StringType)
    val map = Literal.create(Map("a" -> "b"), typeM)
    val nullMap = Literal.create(null, typeM)
    val nullString = Literal.create(null, StringType)

    checkEvaluation(GetMapValue(map, Literal("a")), "b")
    checkEvaluation(GetMapValue(map, nullString), null)
    checkEvaluation(GetMapValue(nullMap, nullString), null)
    checkEvaluation(GetMapValue(map, nullString), null)

    val nonNullMap = Literal.create(Map("a" -> 1), MapType(StringType, IntegerType, false))
    checkEvaluation(GetMapValue(nonNullMap, Literal("a")), 1)

    val nestedMap = Literal.create(Map("a" -> Map("b" -> "c")), MapType(StringType, typeM))
    checkEvaluation(GetMapValue(nestedMap, Literal("a")), Map("b" -> "c"))
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
      errorClass = "DUPLICATED_MAP_KEY",
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
      errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
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
      errorClass = "DUPLICATED_MAP_KEY",
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
      errorClass = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
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
      errorClass = "DUPLICATED_MAP_KEY",
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
