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

package org.apache.spark.sql.catalyst

import java.math.BigInteger
import java.util.{HashSet, LinkedList, List => JList, Map => JMap, Set => JSet}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.reflect.{classTag, ClassTag}

import com.esotericsoftware.kryo.KryoSerializable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.JavaTypeInferenceBeans.{JavaBeanWithGenericBase, JavaBeanWithGenericHierarchy, JavaBeanWithGenericsABC}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, UDTCaseClass, UDTForCaseClass}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.types.{DecimalType, MapType, Metadata, StringType, StructField, StructType}

class DummyBean {
  @BeanProperty var bigInteger: BigInteger = _
}

class GenericCollectionBean {
  @BeanProperty var listOfListOfStrings: JList[JList[String]] = _
  @BeanProperty var mapOfDummyBeans: JMap[String, DummyBean] = _
  @BeanProperty var linkedListOfStrings: LinkedList[String] = _
  @BeanProperty var hashSetOfString: HashSet[String] = _
  @BeanProperty var setOfSetOfStrings: JSet[JSet[String]] = _
}

class ScalaSerializable extends scala.Serializable

class JavaSerializable extends java.io.Serializable

class GenericTypePropertiesBean[S <: JavaSerializable, T <: ScalaSerializable,
  U <: KryoSerializable, V <: UDTCaseClass] {

  @BeanProperty var javaSerializable: S = _
  @BeanProperty var scalaSerializable: T = _
  @BeanProperty var kryoSerializable: U = _
  @BeanProperty var udtSerializable: V = _
}

class LeafBean {
  @BooleanBeanProperty var primitiveBoolean: Boolean = false
  @BeanProperty var primitiveByte: Byte = 0
  @BeanProperty var primitiveShort: Short = 0
  @BeanProperty var primitiveInt: Int = 0
  @BeanProperty var primitiveLong: Long = 0
  @BeanProperty var primitiveFloat: Float = 0
  @BeanProperty var primitiveDouble: Double = 0
  @BeanProperty var boxedBoolean: java.lang.Boolean = false
  @BeanProperty var boxedByte: java.lang.Byte = 0.toByte
  @BeanProperty var boxedShort: java.lang.Short = 0.toShort
  @BeanProperty var boxedInt: java.lang.Integer = 0
  @BeanProperty var boxedLong: java.lang.Long = 0
  @BeanProperty var boxedFloat: java.lang.Float = 0
  @BeanProperty var boxedDouble: java.lang.Double = 0
  @BeanProperty var string: String = _
  @BeanProperty var binary: Array[Byte] = _
  @BeanProperty var bigDecimal: java.math.BigDecimal = _
  @BeanProperty var bigInteger: java.math.BigInteger = _
  @BeanProperty var localDate: java.time.LocalDate = _
  @BeanProperty var date: java.sql.Date = _
  @BeanProperty var instant: java.time.Instant = _
  @BeanProperty var timestamp: java.sql.Timestamp = _
  @BeanProperty var localDateTime: java.time.LocalDateTime = _
  @BeanProperty var duration: java.time.Duration = _
  @BeanProperty var period: java.time.Period = _
  @BeanProperty var monthEnum: java.time.Month = _
  @BeanProperty val readOnlyString = "read-only"
  @BeanProperty var genericNestedBean: JavaBeanWithGenericBase = _
  @BeanProperty var genericNestedBean2: JavaBeanWithGenericsABC[Integer] = _

  var nonNullString: String = "value"
  @javax.annotation.Nonnull
  def getNonNullString: String = nonNullString
  def setNonNullString(v: String): Unit = nonNullString = {
    java.util.Objects.nonNull(v)
    v
  }
}

class ArrayBean {
  @BeanProperty var dummyBeanArray: Array[DummyBean] = _
  @BeanProperty var primitiveIntArray: Array[Int] = _
  @BeanProperty var stringArray: Array[String] = _
}

class UDTBean {
  @BeanProperty var udt: UDTCaseClass = _
}

/**
 * Test suite for Encoders produced by [[JavaTypeInference]].
 */
class JavaTypeInferenceSuite extends SparkFunSuite {

  private def encoderField(
      name: String,
      encoder: AgnosticEncoder[_],
      overrideNullable: Option[Boolean] = None,
      readOnly: Boolean = false): EncoderField = {
    val readPrefix = if (encoder == PrimitiveBooleanEncoder) "is" else "get"
    EncoderField(
      name,
      encoder,
      overrideNullable.getOrElse(encoder.nullable),
      Metadata.empty,
      Option(readPrefix + name.capitalize),
      Option("set" + name.capitalize).filterNot(_ => readOnly))
  }

  private val expectedDummyBeanEncoder =
    JavaBeanEncoder[DummyBean](
      ClassTag(classOf[DummyBean]),
      Seq(encoderField("bigInteger", JavaBigIntEncoder)))

  private val expectedDummyBeanSchema =
    StructType(StructField("bigInteger", DecimalType(38, 0)) :: Nil)

  test("SPARK-41007: JavaTypeInference returns the correct serializer for BigInteger") {
    val encoder = JavaTypeInference.encoderFor(classOf[DummyBean])
    assert(encoder === expectedDummyBeanEncoder)
    assert(encoder.schema === expectedDummyBeanSchema)
  }

  test("resolve schema for class") {
    val (schema, nullable) = JavaTypeInference.inferDataType(classOf[DummyBean])
    assert(nullable)
    assert(schema === expectedDummyBeanSchema)
  }

  test("resolve schema for type") {
    val getter = classOf[GenericCollectionBean].getDeclaredMethods
      .find(_.getName == "getMapOfDummyBeans")
      .get
    val (schema, nullable) = JavaTypeInference.inferDataType(getter.getGenericReturnType)
    val expected = MapType(StringType, expectedDummyBeanSchema, valueContainsNull = true)
    assert(nullable)
    assert(schema === expected)
  }

  test("resolve type parameters for map, list and set") {
    val encoder = JavaTypeInference.encoderFor(classOf[GenericCollectionBean])
    val expected = JavaBeanEncoder(ClassTag(classOf[GenericCollectionBean]), Seq(
      encoderField(
        "hashSetOfString",
        IterableEncoder(
          ClassTag(classOf[HashSet[_]]),
          StringEncoder,
          containsNull = true,
          lenientSerialization = false)),
      encoderField(
        "linkedListOfStrings",
        IterableEncoder(
          ClassTag(classOf[LinkedList[_]]),
          StringEncoder,
          containsNull = true,
          lenientSerialization = false)),
      encoderField(
        "listOfListOfStrings",
        IterableEncoder(
          ClassTag(classOf[JList[_]]),
          IterableEncoder(
            ClassTag(classOf[JList[_]]),
            StringEncoder,
            containsNull = true,
            lenientSerialization = false),
          containsNull = true,
          lenientSerialization = false)),
      encoderField(
        "mapOfDummyBeans",
        MapEncoder(
          ClassTag(classOf[JMap[_, _]]),
          StringEncoder,
          expectedDummyBeanEncoder,
          valueContainsNull = true)),
      encoderField(
        "setOfSetOfStrings",
        IterableEncoder(
          ClassTag(classOf[JSet[_]]),
          IterableEncoder(
            ClassTag(classOf[JSet[_]]),
            StringEncoder,
            containsNull = true,
            lenientSerialization = false),
          containsNull = true,
          lenientSerialization = false))))
    assert(encoder === expected)
  }

  test("resolve bean encoder with generic types as getter/setter") {
    val encoder = JavaTypeInference.encoderFor(classOf[GenericTypePropertiesBean[_, _, _, _]])
    val expected = JavaBeanEncoder(ClassTag(classOf[GenericTypePropertiesBean[_, _, _, _]]), Seq(
      // The order is different from the definition because fields are ordered by name.
      encoderField("javaSerializable",
        Encoders.javaSerialization(classOf[JavaSerializable]).asInstanceOf[AgnosticEncoder[_]]),
      encoderField("kryoSerializable",
        Encoders.kryo(classOf[KryoSerializable]).asInstanceOf[AgnosticEncoder[_]]),
      encoderField("scalaSerializable",
        Encoders.javaSerialization(classOf[ScalaSerializable]).asInstanceOf[AgnosticEncoder[_]]),
      encoderField("udtSerializable", UDTEncoder(new UDTForCaseClass, classOf[UDTForCaseClass]))
    ))
    assert(encoder === expected)
  }

  test("resolve leaf encoders") {
    val encoder = JavaTypeInference.encoderFor(classOf[LeafBean])
    val expected = JavaBeanEncoder(ClassTag(classOf[LeafBean]), Seq(
      // The order is different from the definition because fields are ordered by name.
      encoderField("bigDecimal", DEFAULT_JAVA_DECIMAL_ENCODER),
      encoderField("bigInteger", JavaBigIntEncoder),
      encoderField("binary", BinaryEncoder),
      encoderField("boxedBoolean", BoxedBooleanEncoder),
      encoderField("boxedByte", BoxedByteEncoder),
      encoderField("boxedDouble", BoxedDoubleEncoder),
      encoderField("boxedFloat", BoxedFloatEncoder),
      encoderField("boxedInt", BoxedIntEncoder),
      encoderField("boxedLong", BoxedLongEncoder),
      encoderField("boxedShort", BoxedShortEncoder),
      encoderField("date", STRICT_DATE_ENCODER),
      encoderField("duration", DayTimeIntervalEncoder),
      encoderField("genericNestedBean", JavaBeanEncoder(
        ClassTag(classOf[JavaBeanWithGenericBase]),
        Seq(
          encoderField("attribute", StringEncoder),
          encoderField("value", StringEncoder)
        ))),
      encoderField("genericNestedBean2", JavaBeanEncoder(
        ClassTag(classOf[JavaBeanWithGenericsABC[Integer]]),
        Seq(
          encoderField("propertyA", StringEncoder),
          encoderField("propertyB", BoxedLongEncoder),
          encoderField("propertyC", BoxedIntEncoder)
        ))),
      encoderField("instant", STRICT_INSTANT_ENCODER),
      encoderField("localDate", STRICT_LOCAL_DATE_ENCODER),
      encoderField("localDateTime", LocalDateTimeEncoder),
      encoderField("monthEnum", JavaEnumEncoder(classTag[java.time.Month])),
      encoderField("nonNullString", StringEncoder, overrideNullable = Option(false)),
      encoderField("period", YearMonthIntervalEncoder),
      encoderField("primitiveBoolean", PrimitiveBooleanEncoder),
      encoderField("primitiveByte", PrimitiveByteEncoder),
      encoderField("primitiveDouble", PrimitiveDoubleEncoder),
      encoderField("primitiveFloat", PrimitiveFloatEncoder),
      encoderField("primitiveInt", PrimitiveIntEncoder),
      encoderField("primitiveLong", PrimitiveLongEncoder),
      encoderField("primitiveShort", PrimitiveShortEncoder),
      encoderField("readOnlyString", StringEncoder, readOnly = true),
      encoderField("string", StringEncoder),
      encoderField("timestamp", STRICT_TIMESTAMP_ENCODER)
    ))
    assert(encoder === expected)
  }

  test("resolve array encoders") {
    val encoder = JavaTypeInference.encoderFor(classOf[ArrayBean])
    val expected = JavaBeanEncoder(ClassTag(classOf[ArrayBean]), Seq(
      encoderField("dummyBeanArray", ArrayEncoder(expectedDummyBeanEncoder, containsNull = true)),
      encoderField("primitiveIntArray", ArrayEncoder(PrimitiveIntEncoder, containsNull = false)),
      encoderField("stringArray", ArrayEncoder(StringEncoder, containsNull = true))
    ))
    assert(encoder === expected)
  }

  test("resolve UDT encoders") {
    val encoder = JavaTypeInference.encoderFor(classOf[UDTBean])
    val expected = JavaBeanEncoder(ClassTag(classOf[UDTBean]), Seq(
      encoderField("udt", UDTEncoder(new UDTForCaseClass, classOf[UDTForCaseClass]))
    ))
    assert(encoder === expected)
  }

  test("SPARK-44910: resolve bean with generic base class") {
    val encoder =
      JavaTypeInference.encoderFor(classOf[JavaBeanWithGenericBase])
    val expected =
      JavaBeanEncoder(ClassTag(classOf[JavaBeanWithGenericBase]), Seq(
        encoderField("attribute", StringEncoder),
        encoderField("value", StringEncoder)
      ))
    assert(encoder === expected)
  }

  test("SPARK-44910: resolve bean with hierarchy of generic classes") {
    val encoder =
      JavaTypeInference.encoderFor(classOf[JavaBeanWithGenericHierarchy])
    val expected =
      JavaBeanEncoder(ClassTag(classOf[JavaBeanWithGenericHierarchy]), Seq(
        encoderField("propertyA", StringEncoder),
        encoderField("propertyB", BoxedLongEncoder),
        encoderField("propertyC", BoxedIntEncoder)
      ))
    assert(encoder === expected)
  }
}
