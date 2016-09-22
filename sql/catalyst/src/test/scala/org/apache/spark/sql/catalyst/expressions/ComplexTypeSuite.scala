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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedExtractValue
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


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

    val nestedArray = Literal.create(Seq(Seq(1)), ArrayType(ArrayType(IntegerType)))
    checkEvaluation(GetArrayItem(nestedArray, Literal(0)), Seq(1))
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
    assert(getStructField(struct, "a").nullable === true)
    assert(getStructField(nullStruct_fieldNotNullable, "a").nullable === true)
    assert(getStructField(nullStruct, "a").nullable === true)
  }

  test("GetArrayStructFields") {
    val typeAS = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val arrayStruct = Literal.create(Seq(create_row(1)), typeAS)
    val nullArrayStruct = Literal.create(null, typeAS)

    def getArrayStructFields(expr: Expression, fieldName: String): GetArrayStructFields = {
      expr.dataType match {
        case ArrayType(StructType(fields), containsNull) =>
          val field = fields.find(_.name == fieldName).get
          GetArrayStructFields(expr, field, fields.indexOf(field), fields.length, containsNull)
      }
    }

    checkEvaluation(getArrayStructFields(arrayStruct, "a"), Seq(1))
    checkEvaluation(getArrayStructFields(nullArrayStruct, "a"), null)
  }

  test("CreateArray") {
    val intSeq = Seq(5, 10, 15, 20, 25)
    val longSeq = intSeq.map(_.toLong)
    val strSeq = intSeq.map(_.toString)
    checkEvaluation(CreateArray(intSeq.map(Literal(_))), intSeq, EmptyRow)
    checkEvaluation(CreateArray(longSeq.map(Literal(_))), longSeq, EmptyRow)
    checkEvaluation(CreateArray(strSeq.map(Literal(_))), strSeq, EmptyRow)

    val intWithNull = intSeq.map(Literal(_)) :+ Literal.create(null, IntegerType)
    val longWithNull = longSeq.map(Literal(_)) :+ Literal.create(null, LongType)
    val strWithNull = strSeq.map(Literal(_)) :+ Literal.create(null, StringType)
    checkEvaluation(CreateArray(intWithNull), intSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(longWithNull), longSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(strWithNull), strSeq :+ null, EmptyRow)
    checkEvaluation(CreateArray(Literal.create(null, IntegerType) :: Nil), null :: Nil)
  }

  test("CreateMap") {
    def interlace(keys: Seq[Literal], values: Seq[Literal]): Seq[Literal] = {
      keys.zip(values).flatMap { case (k, v) => Seq(k, v) }
    }

    def createMap(keys: Seq[Any], values: Seq[Any]): Map[Any, Any] = {
      // catalyst map is order-sensitive, so we create ListMap here to preserve the elements order.
      scala.collection.immutable.ListMap(keys.zip(values): _*)
    }

    val intSeq = Seq(5, 10, 15, 20, 25)
    val longSeq = intSeq.map(_.toLong)
    val strSeq = intSeq.map(_.toString)
    checkEvaluation(CreateMap(Nil), Map.empty)
    checkEvaluation(
      CreateMap(interlace(intSeq.map(Literal(_)), longSeq.map(Literal(_)))),
      createMap(intSeq, longSeq))
    checkEvaluation(
      CreateMap(interlace(strSeq.map(Literal(_)), longSeq.map(Literal(_)))),
      createMap(strSeq, longSeq))
    checkEvaluation(
      CreateMap(interlace(longSeq.map(Literal(_)), strSeq.map(Literal(_)))),
      createMap(longSeq, strSeq))

    val strWithNull = strSeq.drop(1).map(Literal(_)) :+ Literal.create(null, StringType)
    checkEvaluation(
      CreateMap(interlace(intSeq.map(Literal(_)), strWithNull)),
      createMap(intSeq, strWithNull.map(_.value)))
    intercept[RuntimeException] {
      checkEvaluationWithoutCodegen(
        CreateMap(interlace(strWithNull, intSeq.map(Literal(_)))),
        null, null)
    }
    intercept[RuntimeException] {
      checkEvalutionWithUnsafeProjection(
        CreateMap(interlace(strWithNull, intSeq.map(Literal(_)))),
        null, null)
    }
  }

  test("CreateStruct") {
    val row = create_row(1, 2, 3)
    val c1 = 'a.int.at(0)
    val c3 = 'c.int.at(2)
    checkEvaluation(CreateStruct(Seq(c1, c3)), create_row(1, 3), row)
    checkEvaluation(CreateStruct(Literal.create(null, LongType) :: Nil), create_row(null))
  }

  test("CreateNamedStruct") {
    val row = create_row(1, 2, 3)
    val c1 = 'a.int.at(0)
    val c3 = 'c.int.at(2)
    checkEvaluation(CreateNamedStruct(Seq("a", c1, "b", c3)), create_row(1, 3), row)
    checkEvaluation(CreateNamedStruct(Seq("a", c1, "b", "y")),
      create_row(1, UTF8String.fromString("y")), row)
    checkEvaluation(CreateNamedStruct(Seq("a", "x", "b", 2.0)),
      create_row(UTF8String.fromString("x"), 2.0))
    checkEvaluation(CreateNamedStruct(Seq("a", Literal.create(null, IntegerType))),
      create_row(null))
  }

  test("test dsl for complex type") {
    def quickResolve(u: UnresolvedExtractValue): Expression = {
      ExtractValue(u.child, u.extraction, _ == _)
    }

    checkEvaluation(quickResolve('c.map(MapType(StringType, StringType)).at(0).getItem("a")),
      "b", create_row(Map("a" -> "b")))
    checkEvaluation(quickResolve('c.array(StringType).at(0).getItem(1)),
      "b", create_row(Seq("a", "b")))
    checkEvaluation(quickResolve('c.struct('a.int).at(0).getField("a")),
      1, create_row(create_row(1)))
  }

  test("error message of ExtractValue") {
    val structType = StructType(StructField("a", StringType, true) :: Nil)
    val otherType = StringType

    def checkErrorMessage(
      childDataType: DataType,
      fieldDataType: DataType,
      errorMesage: String): Unit = {
      val e = intercept[org.apache.spark.sql.AnalysisException] {
        ExtractValue(
          Literal.create(null, childDataType),
          Literal.create(null, fieldDataType),
          _ == _)
      }
      assert(e.getMessage().contains(errorMesage))
    }

    checkErrorMessage(structType, IntegerType, "Field name should be String Literal")
    checkErrorMessage(otherType, StringType, "Can't extract value from")
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
    checkMetadata(CreateStructUnsafe(Seq(a, b)))
    checkMetadata(CreateNamedStructUnsafe(Seq("a", a, "b", b)))
  }

  test("StringToMap") {
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

    // arguments checking
    assert(new StringToMap(Literal("a:1,b:2,c:3")).checkInputDataTypes().isSuccess)
    assert(new StringToMap(Literal(null)).checkInputDataTypes().isFailure)
    assert(new StringToMap(Literal("a:1,b:2,c:3"), Literal(null)).checkInputDataTypes().isFailure)
    assert(StringToMap(Literal("a:1,b:2,c:3"), Literal(null), Literal(null))
      .checkInputDataTypes().isFailure)
    assert(new StringToMap(Literal(null), Literal(null)).checkInputDataTypes().isFailure)

    assert(new StringToMap(Literal("a:1_b:2_c:3"), NonFoldableLiteral("_"))
        .checkInputDataTypes().isFailure)
    assert(
      new StringToMap(Literal("a=1_b=2_c=3"), Literal("_"), NonFoldableLiteral("="))
        .checkInputDataTypes().isFailure)
  }
}
