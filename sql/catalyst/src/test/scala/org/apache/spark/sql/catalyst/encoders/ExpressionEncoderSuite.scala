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

package org.apache.spark.sql.catalyst.encoders

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import java.util.Arrays

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.{FooClassWithEnum, FooEnum, OptionalData, PrimitiveData}
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ClosureCleaner

case class RepeatedStruct(s: Seq[PrimitiveData])

case class NestedArray(a: Array[Array[Int]]) {
  override def hashCode(): Int =
    java.util.Arrays.deepHashCode(a.asInstanceOf[Array[AnyRef]])

  override def equals(other: Any): Boolean = other match {
    case NestedArray(otherArray) =>
      java.util.Arrays.deepEquals(
        a.asInstanceOf[Array[AnyRef]],
        otherArray.asInstanceOf[Array[AnyRef]])
    case _ => false
  }
}

case class BoxedData(
    intField: java.lang.Integer,
    longField: java.lang.Long,
    doubleField: java.lang.Double,
    floatField: java.lang.Float,
    shortField: java.lang.Short,
    byteField: java.lang.Byte,
    booleanField: java.lang.Boolean)

case class RepeatedData(
    arrayField: Seq[Int],
    arrayFieldContainsNull: Seq[java.lang.Integer],
    mapField: scala.collection.Map[Int, Long],
    mapFieldNull: scala.collection.Map[Int, java.lang.Long],
    structField: PrimitiveData)

/** For testing Kryo serialization based encoder. */
class KryoSerializable(val value: Int) {
  override def hashCode(): Int = value

  override def equals(other: Any): Boolean = other match {
    case that: KryoSerializable => this.value == that.value
    case _ => false
  }
}

/** For testing Java serialization based encoder. */
class JavaSerializable(val value: Int) extends Serializable {
  override def hashCode(): Int = value

  override def equals(other: Any): Boolean = other match {
    case that: JavaSerializable => this.value == that.value
    case _ => false
  }
}

/** For testing UDT for a case class */
@SQLUserDefinedType(udt = classOf[UDTForCaseClass])
case class UDTCaseClass(uri: java.net.URI)

class UDTForCaseClass extends UserDefinedType[UDTCaseClass] {

  override def sqlType: DataType = StringType

  override def serialize(obj: UDTCaseClass): UTF8String = {
    UTF8String.fromString(obj.uri.toString)
  }

  override def userClass: Class[UDTCaseClass] = classOf[UDTCaseClass]

  override def deserialize(datum: Any): UDTCaseClass = datum match {
    case uri: UTF8String => UDTCaseClass(new java.net.URI(uri.toString))
  }
}

case class Bar(i: Any)
case class Foo(i: Bar) extends AnyVal
case class PrimitiveValueClass(wrapped: Int) extends AnyVal
case class ReferenceValueClass(wrapped: ReferenceValueClass.Container) extends AnyVal
object ReferenceValueClass {
  case class Container(data: Int)
}
case class IntAndString(i: Int, s: String)

class ExpressionEncoderSuite extends CodegenInterpretedPlanTest with AnalysisTest {
  OuterScopes.addOuterScope(this)

  implicit def encoder[T : TypeTag]: ExpressionEncoder[T] = verifyNotLeakingReflectionObjects {
    ExpressionEncoder()
  }

  // test flat encoders
  encodeDecodeTest(false, "primitive boolean")
  encodeDecodeTest(-3.toByte, "primitive byte")
  encodeDecodeTest(-3.toShort, "primitive short")
  encodeDecodeTest(-3, "primitive int")
  encodeDecodeTest(-3L, "primitive long")
  encodeDecodeTest(-3.7f, "primitive float")
  encodeDecodeTest(-3.7, "primitive double")

  encodeDecodeTest(java.lang.Boolean.FALSE, "boxed boolean")
  encodeDecodeTest(java.lang.Byte.valueOf(-3: Byte), "boxed byte")
  encodeDecodeTest(java.lang.Short.valueOf(-3: Short), "boxed short")
  encodeDecodeTest(java.lang.Integer.valueOf(-3), "boxed int")
  encodeDecodeTest(java.lang.Long.valueOf(-3L), "boxed long")
  encodeDecodeTest(java.lang.Float.valueOf(-3.7f), "boxed float")
  encodeDecodeTest(java.lang.Double.valueOf(-3.7), "boxed double")

  encodeDecodeTest(BigDecimal("32131413.211321313"), "scala decimal")
  encodeDecodeTest(new java.math.BigDecimal("231341.23123"), "java decimal")
  encodeDecodeTest(BigInt("23134123123"), "scala biginteger")
  encodeDecodeTest(new BigInteger("23134123123"), "java BigInteger")
  encodeDecodeTest(Decimal("32131413.211321313"), "catalyst decimal")

  encodeDecodeTest("hello", "string")
  encodeDecodeTest(Date.valueOf("2012-12-23"), "date")
  encodeDecodeTest(Timestamp.valueOf("2016-01-29 10:00:00"), "timestamp")
  encodeDecodeTest(Array(Timestamp.valueOf("2016-01-29 10:00:00")), "array of timestamp")
  encodeDecodeTest(Array[Byte](13, 21, -23), "binary")

  encodeDecodeTest(Seq(31, -123, 4), "seq of int")
  encodeDecodeTest(Seq("abc", "xyz"), "seq of string")
  encodeDecodeTest(Seq("abc", null, "xyz"), "seq of string with null")
  encodeDecodeTest(Seq.empty[Int], "empty seq of int")
  encodeDecodeTest(Seq.empty[String], "empty seq of string")

  encodeDecodeTest(Seq(Seq(31, -123), null, Seq(4, 67)), "seq of seq of int")
  encodeDecodeTest(Seq(Seq("abc", "xyz"), Seq[String](null), null, Seq("1", null, "2")),
    "seq of seq of string")

  encodeDecodeTest(Array(31, -123, 4), "array of int")
  encodeDecodeTest(Array("abc", "xyz"), "array of string")
  encodeDecodeTest(Array("a", null, "x"), "array of string with null")
  encodeDecodeTest(Array.empty[Int], "empty array of int")
  encodeDecodeTest(Array.empty[String], "empty array of string")

  encodeDecodeTest(Array(Array(31, -123), null, Array(4, 67)), "array of array of int")
  encodeDecodeTest(Array(Array("abc", "xyz"), Array[String](null), null, Array("1", null, "2")),
    "array of array of string")

  encodeDecodeTest(Map(1 -> "a", 2 -> "b"), "map")
  encodeDecodeTest(Map(1 -> "a", 2 -> null), "map with null")
  encodeDecodeTest(Map(1 -> Map("a" -> 1), 2 -> Map("b" -> 2)), "map of map")
  encodeDecodeTest(Map(1 -> IntAndString(1, "a")), "map with case class as value")
  encodeDecodeTest(Map(IntAndString(1, "a") -> 1), "map with case class as key")
  encodeDecodeTest(Map(IntAndString(1, "a") -> IntAndString(2, "b")),
    "map with case class as key and value")

  encodeDecodeTest(Tuple1[Seq[Int]](null), "null seq in tuple")
  encodeDecodeTest(Tuple1[Map[String, String]](null), "null map in tuple")

  encodeDecodeTest(List(1, 2), "list of int")
  encodeDecodeTest(List("a", null), "list with String and null")

  encodeDecodeTest(
    UDTCaseClass(new java.net.URI("http://spark.apache.org/")), "udt with case class")

  // Kryo encoders
  encodeDecodeTest("hello", "kryo string")(encoderFor(Encoders.kryo[String]))
  encodeDecodeTest(new KryoSerializable(15), "kryo object")(
    encoderFor(Encoders.kryo[KryoSerializable]))

  // Java encoders
  encodeDecodeTest("hello", "java string")(encoderFor(Encoders.javaSerialization[String]))
  encodeDecodeTest(new JavaSerializable(15), "java object")(
    encoderFor(Encoders.javaSerialization[JavaSerializable]))

  // test product encoders
  private def productTest[T <: Product : ExpressionEncoder](
      input: T, useFallback: Boolean = false): Unit = {
    encodeDecodeTest(input, input.getClass.getSimpleName, useFallback)
  }

  case class InnerClass(i: Int)
  productTest(InnerClass(1))
  encodeDecodeTest(Array(InnerClass(1)), "array of inner class")

  encodeDecodeTest(Array(Option(InnerClass(1))), "array of optional inner class")

  // holder class to trigger Class.getSimpleName issue
  object MalformedClassObject extends Serializable {
    case class MalformedNameExample(x: Int)
  }

  {
    OuterScopes.addOuterScope(MalformedClassObject)
    encodeDecodeTest(
      MalformedClassObject.MalformedNameExample(42),
      "nested Scala class should work",
      useFallback = true)
  }

  object OuterLevelWithVeryVeryVeryLongClassName1 {
    object OuterLevelWithVeryVeryVeryLongClassName2 {
      object OuterLevelWithVeryVeryVeryLongClassName3 {
        object OuterLevelWithVeryVeryVeryLongClassName4 {
          object OuterLevelWithVeryVeryVeryLongClassName5 {
            object OuterLevelWithVeryVeryVeryLongClassName6 {
              object OuterLevelWithVeryVeryVeryLongClassName7 {
                object OuterLevelWithVeryVeryVeryLongClassName8 {
                  object OuterLevelWithVeryVeryVeryLongClassName9 {
                    object OuterLevelWithVeryVeryVeryLongClassName10 {
                      object OuterLevelWithVeryVeryVeryLongClassName11 {
                        object OuterLevelWithVeryVeryVeryLongClassName12 {
                          object OuterLevelWithVeryVeryVeryLongClassName13 {
                            object OuterLevelWithVeryVeryVeryLongClassName14 {
                              object OuterLevelWithVeryVeryVeryLongClassName15 {
                                object OuterLevelWithVeryVeryVeryLongClassName16 {
                                  object OuterLevelWithVeryVeryVeryLongClassName17 {
                                    object OuterLevelWithVeryVeryVeryLongClassName18 {
                                      object OuterLevelWithVeryVeryVeryLongClassName19 {
                                        object OuterLevelWithVeryVeryVeryLongClassName20 {
                                          case class MalformedNameExample(x: Int)
                                        }}}}}}}}}}}}}}}}}}}}

  {
    OuterScopes.addOuterScope(
      OuterLevelWithVeryVeryVeryLongClassName1
        .OuterLevelWithVeryVeryVeryLongClassName2
        .OuterLevelWithVeryVeryVeryLongClassName3
        .OuterLevelWithVeryVeryVeryLongClassName4
        .OuterLevelWithVeryVeryVeryLongClassName5
        .OuterLevelWithVeryVeryVeryLongClassName6
        .OuterLevelWithVeryVeryVeryLongClassName7
        .OuterLevelWithVeryVeryVeryLongClassName8
        .OuterLevelWithVeryVeryVeryLongClassName9
        .OuterLevelWithVeryVeryVeryLongClassName10
        .OuterLevelWithVeryVeryVeryLongClassName11
        .OuterLevelWithVeryVeryVeryLongClassName12
        .OuterLevelWithVeryVeryVeryLongClassName13
        .OuterLevelWithVeryVeryVeryLongClassName14
        .OuterLevelWithVeryVeryVeryLongClassName15
        .OuterLevelWithVeryVeryVeryLongClassName16
        .OuterLevelWithVeryVeryVeryLongClassName17
        .OuterLevelWithVeryVeryVeryLongClassName18
        .OuterLevelWithVeryVeryVeryLongClassName19
        .OuterLevelWithVeryVeryVeryLongClassName20)
    encodeDecodeTest(
      OuterLevelWithVeryVeryVeryLongClassName1
        .OuterLevelWithVeryVeryVeryLongClassName2
        .OuterLevelWithVeryVeryVeryLongClassName3
        .OuterLevelWithVeryVeryVeryLongClassName4
        .OuterLevelWithVeryVeryVeryLongClassName5
        .OuterLevelWithVeryVeryVeryLongClassName6
        .OuterLevelWithVeryVeryVeryLongClassName7
        .OuterLevelWithVeryVeryVeryLongClassName8
        .OuterLevelWithVeryVeryVeryLongClassName9
        .OuterLevelWithVeryVeryVeryLongClassName10
        .OuterLevelWithVeryVeryVeryLongClassName11
        .OuterLevelWithVeryVeryVeryLongClassName12
        .OuterLevelWithVeryVeryVeryLongClassName13
        .OuterLevelWithVeryVeryVeryLongClassName14
        .OuterLevelWithVeryVeryVeryLongClassName15
        .OuterLevelWithVeryVeryVeryLongClassName16
        .OuterLevelWithVeryVeryVeryLongClassName17
        .OuterLevelWithVeryVeryVeryLongClassName18
        .OuterLevelWithVeryVeryVeryLongClassName19
        .OuterLevelWithVeryVeryVeryLongClassName20
        .MalformedNameExample(42),
      "deeply nested Scala class should work",
      useFallback = true)
  }

  productTest(PrimitiveData(1, 1, 1, 1, 1, 1, true))

  productTest(
    OptionalData(Some(2), Some(2), Some(2), Some(2), Some(2), Some(2), Some(true),
      Some(PrimitiveData(1, 1, 1, 1, 1, 1, true)), Some(new CalendarInterval(1, 2, 3))))

  productTest(OptionalData(None, None, None, None, None, None, None, None, None))

  encodeDecodeTest(Seq(Some(1), None), "Option in array")
  encodeDecodeTest(Map(1 -> Some(10L), 2 -> Some(20L), 3 -> None), "Option in map",
    useFallback = true)

  productTest(BoxedData(1, 1L, 1.0, 1.0f, 1.toShort, 1.toByte, true))

  productTest(BoxedData(null, null, null, null, null, null, null))

  productTest(RepeatedStruct(PrimitiveData(1, 1, 1, 1, 1, 1, true) :: Nil))

  productTest((1, "test", PrimitiveData(1, 1, 1, 1, 1, 1, true)))

  productTest(
    RepeatedData(
      Seq(1, 2),
      Seq(Integer.valueOf(1), null, Integer.valueOf(2)),
      Map(1 -> 2L),
      Map(1 -> null),
      PrimitiveData(1, 1, 1, 1, 1, 1, true)))

  productTest(NestedArray(Array(Array(1, -2, 3), null, Array(4, 5, -6))), useFallback = true)

  productTest(("Seq[(String, String)]",
    Seq(("a", "b"))))
  productTest(("Seq[(Int, Int)]",
    Seq((1, 2))))
  productTest(("Seq[(Long, Long)]",
    Seq((1L, 2L))))
  productTest(("Seq[(Float, Float)]",
    Seq((1.toFloat, 2.toFloat))))
  productTest(("Seq[(Double, Double)]",
    Seq((1.toDouble, 2.toDouble))))
  productTest(("Seq[(Short, Short)]",
    Seq((1.toShort, 2.toShort))))
  productTest(("Seq[(Byte, Byte)]",
    Seq((1.toByte, 2.toByte))))
  productTest(("Seq[(Boolean, Boolean)]",
    Seq((true, false))))

  productTest(("ArrayBuffer[(String, String)]",
    ArrayBuffer(("a", "b"))))
  productTest(("ArrayBuffer[(Int, Int)]",
    ArrayBuffer((1, 2))))
  productTest(("ArrayBuffer[(Long, Long)]",
    ArrayBuffer((1L, 2L))))
  productTest(("ArrayBuffer[(Float, Float)]",
    ArrayBuffer((1.toFloat, 2.toFloat))))
  productTest(("ArrayBuffer[(Double, Double)]",
    ArrayBuffer((1.toDouble, 2.toDouble))))
  productTest(("ArrayBuffer[(Short, Short)]",
    ArrayBuffer((1.toShort, 2.toShort))))
  productTest(("ArrayBuffer[(Byte, Byte)]",
    ArrayBuffer((1.toByte, 2.toByte))))
  productTest(("ArrayBuffer[(Boolean, Boolean)]",
    ArrayBuffer((true, false))))

  productTest(("Seq[Seq[(Int, Int)]]",
    Seq(Seq((1, 2)))))

  // test for ExpressionEncoder.tuple
  encodeDecodeTest(
    1 -> 10L,
    "tuple with 2 flat encoders")(
    ExpressionEncoder.tuple(ExpressionEncoder[Int], ExpressionEncoder[Long]))

  encodeDecodeTest(
    (PrimitiveData(1, 1, 1, 1, 1, 1, true), (3, 30L)),
    "tuple with 2 product encoders")(
    ExpressionEncoder.tuple(ExpressionEncoder[PrimitiveData], ExpressionEncoder[(Int, Long)]))

  encodeDecodeTest(
    (PrimitiveData(1, 1, 1, 1, 1, 1, true), 3),
    "tuple with flat encoder and product encoder")(
    ExpressionEncoder.tuple(ExpressionEncoder[PrimitiveData], ExpressionEncoder[Int]))

  encodeDecodeTest(
    (3, PrimitiveData(1, 1, 1, 1, 1, 1, true)),
    "tuple with product encoder and flat encoder")(
    ExpressionEncoder.tuple(ExpressionEncoder[Int], ExpressionEncoder[PrimitiveData]))

  encodeDecodeTest(
    (1, (10, 100L)),
    "nested tuple encoder") {
    val intEnc = ExpressionEncoder[Int]
    val longEnc = ExpressionEncoder[Long]
    ExpressionEncoder.tuple(intEnc, ExpressionEncoder.tuple(intEnc, longEnc))
  }

  encodeDecodeTest(
    PrimitiveValueClass(42), "primitive value class")

  encodeDecodeTest(
    ReferenceValueClass(ReferenceValueClass.Container(1)), "reference value class")

  encodeDecodeTest(Option(31), "option of int")
  encodeDecodeTest(Option.empty[Int], "empty option of int")
  encodeDecodeTest(Option("abc"), "option of string")
  encodeDecodeTest(Option.empty[String], "empty option of string")

  productTest(("UDT", new ExamplePoint(0.1, 0.2)))

  test("AnyVal class with Any fields") {
    val exception = intercept[UnsupportedOperationException](implicitly[ExpressionEncoder[Foo]])
    val errorMsg = exception.getMessage
    assert(errorMsg.contains("root class: \"org.apache.spark.sql.catalyst.encoders.Foo\""))
    assert(errorMsg.contains("No Encoder found for Any"))
  }

  test("nullable of encoder schema") {
    def checkNullable[T: ExpressionEncoder](nullable: Boolean*): Unit = {
      assert(implicitly[ExpressionEncoder[T]].schema.map(_.nullable) === nullable.toSeq)
    }

    // test for flat encoders
    checkNullable[Int](false)
    checkNullable[Option[Int]](true)
    checkNullable[java.lang.Integer](true)
    checkNullable[String](true)

    // test for product encoders
    checkNullable[(String, Int)](true, false)
    checkNullable[(Int, java.lang.Long)](false, true)

    // test for nested product encoders
    {
      val schema = ExpressionEncoder[(Int, (String, Int))].schema
      assert(schema(0).nullable === false)
      assert(schema(1).nullable)
      assert(schema(1).dataType.asInstanceOf[StructType](0).nullable)
      assert(schema(1).dataType.asInstanceOf[StructType](1).nullable === false)
    }

    // test for tupled encoders
    {
      val schema = ExpressionEncoder.tuple(
        ExpressionEncoder[Int],
        ExpressionEncoder[(String, Int)]).schema
      assert(schema(0).nullable === false)
      assert(schema(1).nullable)
      assert(schema(1).dataType.asInstanceOf[StructType](0).nullable)
      assert(schema(1).dataType.asInstanceOf[StructType](1).nullable === false)
    }
  }

  test("nullable of encoder serializer") {
    def checkNullable[T: Encoder](nullable: Boolean): Unit = {
      assert(encoderFor[T].objSerializer.nullable === nullable)
    }

    // test for flat encoders
    checkNullable[Int](false)
    checkNullable[Option[Int]](true)
    checkNullable[java.lang.Integer](true)
    checkNullable[String](true)
  }

  test("null check for map key: String") {
    val toRow = ExpressionEncoder[Map[String, Int]]().createSerializer()
    val e = intercept[RuntimeException](toRow(Map(("a", 1), (null, 2))))
    assert(e.getMessage.contains("Cannot use null as map key"))
  }

  test("null check for map key: Integer") {
    val toRow = ExpressionEncoder[Map[Integer, String]]().createSerializer()
    val e = intercept[RuntimeException](toRow(Map((1, "a"), (null, "b"))))
    assert(e.getMessage.contains("Cannot use null as map key"))
  }

  test("throw exception for tuples with more than 22 elements") {
    val encoders = (0 to 22).map(_ => Encoders.scalaInt.asInstanceOf[ExpressionEncoder[_]])

    val e = intercept[UnsupportedOperationException] {
      ExpressionEncoder.tuple(encoders)
    }
    assert(e.getMessage.contains("tuple with more than 22 elements are not supported"))
  }

  encodeDecodeTest((1, FooEnum.E1), "Tuple with Int and scala Enum")
  encodeDecodeTest((null, FooEnum.E1, FooEnum.E2), "Tuple with Null and scala Enum")
  encodeDecodeTest(Seq(FooEnum.E1, null), "Seq with scala Enum")
  encodeDecodeTest(Map("key" -> FooEnum.E1), "Map with String key and scala Enum",
    useFallback = true)
  encodeDecodeTest(Map(FooEnum.E1 -> "value"), "Map with scala Enum key and String value",
    useFallback = true)
  encodeDecodeTest(FooClassWithEnum(1, FooEnum.E1), "case class with Int and scala Enum")
  encodeDecodeTest(FooEnum.E1, "scala Enum")

  // Scala / Java big decimals ----------------------------------------------------------

  encodeDecodeTest(BigDecimal(("9" * 20) + "." + "9" * 18),
    "scala decimal within precision/scale limit")
  encodeDecodeTest(new java.math.BigDecimal(("9" * 20) + "." + "9" * 18),
    "java decimal within precision/scale limit")

  encodeDecodeTest(-BigDecimal(("9" * 20) + "." + "9" * 18),
    "negative scala decimal within precision/scale limit")
  encodeDecodeTest(new java.math.BigDecimal(("9" * 20) + "." + "9" * 18).negate,
    "negative java decimal within precision/scale limit")

  testOverflowingBigNumeric(BigDecimal("1" * 21), "scala big decimal")
  testOverflowingBigNumeric(new java.math.BigDecimal("1" * 21), "java big decimal")

  testOverflowingBigNumeric(-BigDecimal("1" * 21), "negative scala big decimal")
  testOverflowingBigNumeric(new java.math.BigDecimal("1" * 21).negate, "negative java big decimal")

  testOverflowingBigNumeric(BigDecimal(("1" * 21) + ".123"),
    "scala big decimal with fractional part")
  testOverflowingBigNumeric(new java.math.BigDecimal(("1" * 21) + ".123"),
    "java big decimal with fractional part")

  testOverflowingBigNumeric(BigDecimal(("1" * 21)  + "." + "9999" * 100),
    "scala big decimal with long fractional part")
  testOverflowingBigNumeric(new java.math.BigDecimal(("1" * 21)  + "." + "9999" * 100),
    "java big decimal with long fractional part")

  // Scala / Java big integers ----------------------------------------------------------

  encodeDecodeTest(BigInt("9" * 38), "scala big integer within precision limit")
  encodeDecodeTest(new BigInteger("9" * 38), "java big integer within precision limit")

  encodeDecodeTest(-BigInt("9" * 38),
    "negative scala big integer within precision limit")
  encodeDecodeTest(new BigInteger("9" * 38).negate(),
    "negative java big integer within precision limit")

  testOverflowingBigNumeric(BigInt("1" * 39), "scala big int")
  testOverflowingBigNumeric(new BigInteger("1" * 39), "java big integer")

  testOverflowingBigNumeric(-BigInt("1" * 39), "negative scala big int")
  testOverflowingBigNumeric(new BigInteger("1" * 39).negate, "negative java big integer")

  testOverflowingBigNumeric(BigInt("9" * 100), "scala very large big int")
  testOverflowingBigNumeric(new BigInteger("9" * 100), "java very big int")

  private def testOverflowingBigNumeric[T: TypeTag](bigNumeric: T, testName: String): Unit = {
    Seq(true, false).foreach { ansiEnabled =>
      testAndVerifyNotLeakingReflectionObjects(
        s"overflowing $testName, ansiEnabled=$ansiEnabled") {
        withSQLConf(
          SQLConf.ANSI_ENABLED.key -> ansiEnabled.toString
        ) {
          // Need to construct Encoder here rather than implicitly resolving it
          // so that SQLConf changes are respected.
          val encoder = ExpressionEncoder[T]()
          val toRow = encoder.createSerializer()
          if (!ansiEnabled) {
            val fromRow = encoder.resolveAndBind().createDeserializer()
            val convertedBack = fromRow(toRow(bigNumeric))
            assert(convertedBack === null)
          } else {
            val e = intercept[RuntimeException] {
              toRow(bigNumeric)
            }
            assert(e.getMessage.contains("Error while encoding"))
            assert(e.getCause.getClass === classOf[ArithmeticException])
          }
        }
      }
    }
  }

  private def encodeDecodeTest[T : ExpressionEncoder](
      input: T,
      testName: String,
      useFallback: Boolean = false): Unit = {
    testAndVerifyNotLeakingReflectionObjects(s"encode/decode for $testName: $input", useFallback) {
      val encoder = implicitly[ExpressionEncoder[T]]

      // Make sure encoder is serializable.
      ClosureCleaner.clean((s: String) => encoder.getClass.getName)

      val row = encoder.createSerializer().apply(input)
      val schema = encoder.schema.toAttributes
      val boundEncoder = encoder.resolveAndBind()
      val convertedBack = try boundEncoder.createDeserializer().apply(row) catch {
        case e: Exception =>
          fail(
           s"""Exception thrown while decoding
              |Converted: $row
              |Schema: ${schema.mkString(",")}
              |${encoder.schema.treeString}
              |
              |Encoder:
              |$boundEncoder
              |
            """.stripMargin, e)
      }

      // Test the correct resolution of serialization / deserialization.
      val attr = AttributeReference("obj", encoder.deserializer.dataType)()
      val plan = LocalRelation(attr).serialize[T].deserialize[T]
      assertAnalysisSuccess(plan)

      val isCorrect = (input, convertedBack) match {
        case (b1: Array[Byte], b2: Array[Byte]) => Arrays.equals(b1, b2)
        case (b1: Array[Int], b2: Array[Int]) => Arrays.equals(b1, b2)
        case (b1: Array[_], b2: Array[_]) =>
          Arrays.deepEquals(b1.asInstanceOf[Array[AnyRef]], b2.asInstanceOf[Array[AnyRef]])
        case (left: Comparable[_], right: Comparable[_]) =>
          left.asInstanceOf[Comparable[Any]].compareTo(right) == 0
        case _ => input == convertedBack
      }

      if (!isCorrect) {
        val types = convertedBack match {
          case c: Product =>
            c.productIterator.filter(_ != null).map(_.getClass.getName).mkString(",")
          case other => other.getClass.getName
        }

        val encodedData = try {
          row.toSeq(encoder.schema).zip(schema).map {
            case (a: ArrayData, AttributeReference(_, ArrayType(et, _), _, _)) =>
              a.toArray[Any](et).toSeq
            case (other, _) =>
              other
          }.mkString("[", ",", "]")
        } catch {
          case e: Throwable => s"Failed to toSeq: $e"
        }

        fail(
          s"""Encoded/Decoded data does not match input data
             |
             |in:  $input
             |out: $convertedBack
             |types: $types
             |
             |Encoded Data: $encodedData
             |Schema: ${schema.mkString(",")}
             |${encoder.schema.treeString}
             |
             |fromRow Expressions:
             |${boundEncoder.deserializer.treeString}
         """.stripMargin)
      }
    }
  }

  /**
   * Verify the size of scala.reflect.runtime.JavaUniverse.undoLog before and after `func` to
   * ensure we don't leak Scala reflection garbage.
   *
   * @see org.apache.spark.sql.catalyst.ScalaReflection.cleanUpReflectionObjects
   */
  private def verifyNotLeakingReflectionObjects[T](func: => T): T = {
    def undoLogSize: Int = {
      scala.reflect.runtime.universe
        .asInstanceOf[scala.reflect.runtime.JavaUniverse].undoLog.log.size
    }

    val previousUndoLogSize = undoLogSize
    val r = func
    assert(previousUndoLogSize == undoLogSize)
    r
  }

  private def testAndVerifyNotLeakingReflectionObjects(
      testName: String, useFallback: Boolean = false)(testFun: => Any): Unit = {
    if (useFallback) {
      testFallback(testName) {
        verifyNotLeakingReflectionObjects(testFun)
      }
    } else {
      test(testName) {
        verifyNotLeakingReflectionObjects(testFun)
      }
    }
  }
}
