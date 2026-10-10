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

import test.org.apache.spark.sql.JavaRecordEncoderTestData._

import org.apache.spark.{SparkFunSuite, SparkRuntimeException, SparkUnsupportedOperationException}
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.JavaTypeInferenceBeans.{Bar, Foo, JavaBeanWithGenericBase, JavaBeanWithGenericHierarchy, JavaBeanWithGenericsABC, StringBarWrapper, StringFooWrapper}
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

  test("SPARK-46679: resolve generics with multi-level inheritance") {
    val encoder = JavaTypeInference.encoderFor(classOf[StringFooWrapper])
    val expected = JavaBeanEncoder(ClassTag(classOf[StringFooWrapper]), Seq(
      encoderField("foo", JavaBeanEncoder(
        ClassTag(classOf[Foo[String]]),
        Seq(encoderField("t", StringEncoder))
      ))
    ))
    assert(encoder === expected)
  }

  test("SPARK-46679: resolve generics with multi-level inheritance same type names") {
    val encoder = JavaTypeInference.encoderFor(classOf[StringBarWrapper])
    val expected = JavaBeanEncoder(ClassTag(classOf[StringBarWrapper]), Seq(
      encoderField("bar", JavaBeanEncoder(
        ClassTag(classOf[Bar[String]]),
        Seq(encoderField("t", StringEncoder))
      ))
    ))
    assert(encoder === expected)
  }

  private def recordField(
      name: String,
      encoder: AgnosticEncoder[_],
      nullable: Option[Boolean] = None): EncoderField = {
    EncoderField(name, encoder, nullable.getOrElse(encoder.nullable), Metadata.empty,
      readMethod = Some(name), writeMethod = None)
  }

  private def listEncoder(element: AgnosticEncoder[_]): AgnosticEncoder[_] =
    IterableEncoder(ClassTag(classOf[JList[_]]), element, containsNull = true,
      lenientSerialization = false)

  private val expectedAddressEncoder = JavaRecordEncoder[Address](ClassTag(classOf[Address]), Seq(
    recordField("street", StringEncoder),
    recordField("city", StringEncoder)))

  private def genericBoxEncoder(value: AgnosticEncoder[_]): AgnosticEncoder[_] =
    JavaRecordEncoder(ClassTag(classOf[GenericBox[_]]), Seq(recordField("value", value)))

  test("SPARK-55396: resolve record encoders in component declaration order") {
    val expected = JavaRecordEncoder(ClassTag(classOf[Person]), Seq(
      recordField("name", StringEncoder),
      recordField("age", PrimitiveIntEncoder),
      recordField("address", expectedAddressEncoder)))
    assert(JavaTypeInference.encoderFor(classOf[Person]) === expected)
    assert(Encoders.record(classOf[Person]) === expected)
    assert(Encoders.bean(classOf[Person]) === expected)
  }

  test("SPARK-55396: resolve record leaf and collection component encoders") {
    assert(JavaTypeInference.encoderFor(classOf[LeafTypesRecord]) ===
      JavaRecordEncoder(ClassTag(classOf[LeafTypesRecord]), Seq(
        recordField("flag", PrimitiveBooleanEncoder),
        recordField("dec", DEFAULT_JAVA_DECIMAL_ENCODER),
        recordField("date", STRICT_LOCAL_DATE_ENCODER),
        recordField("instant", STRICT_INSTANT_ENCODER),
        recordField("color", JavaEnumEncoder(ClassTag(classOf[Color]))))))
    assert(JavaTypeInference.encoderFor(classOf[CollectionRecord]) ===
      JavaRecordEncoder(ClassTag(classOf[CollectionRecord]), Seq(
        recordField("items", listEncoder(StringEncoder)),
        recordField("numbers", IterableEncoder(ClassTag(classOf[JSet[_]]), BoxedIntEncoder,
          containsNull = true, lenientSerialization = false)),
        recordField("addresses", listEncoder(expectedAddressEncoder)),
        recordField("addressesByName", MapEncoder(ClassTag(classOf[JMap[_, _]]), StringEncoder,
          expectedAddressEncoder, valueContainsNull = true)))))
    assert(JavaTypeInference.encoderFor(classOf[ArrayRecord]) ===
      JavaRecordEncoder(ClassTag(classOf[ArrayRecord]), Seq(
        recordField("ints", ArrayEncoder(PrimitiveIntEncoder, containsNull = false)),
        recordField("strings", ArrayEncoder(StringEncoder, containsNull = true)),
        recordField("addresses", ArrayEncoder(expectedAddressEncoder, containsNull = true)))))
  }

  test("SPARK-55396: @Nonnull record components are not nullable") {
    assert(JavaTypeInference.encoderFor(classOf[NonnullRecord]) ===
      JavaRecordEncoder(ClassTag(classOf[NonnullRecord]), Seq(
        recordField("name", StringEncoder, nullable = Some(false)),
        recordField("count", BoxedIntEncoder, nullable = Some(false)),
        recordField("note", StringEncoder))))
  }

  test("SPARK-55396: only record components become fields") {
    assert(JavaTypeInference.encoderFor(classOf[WithExtraMethods]) ===
      JavaRecordEncoder(ClassTag(classOf[WithExtraMethods]), Seq(
        recordField("a", PrimitiveIntEncoder))))
    assert(JavaTypeInference.encoderFor(classOf[EmptyRecord]) ===
      JavaRecordEncoder(ClassTag(classOf[EmptyRecord]), Nil))
  }

  test("SPARK-55396: records and JavaBeans nest in each other") {
    val beanEncoder = JavaBeanEncoder(ClassTag(classOf[SimpleBean]), Seq(
      encoderField("name", StringEncoder),
      encoderField("value", PrimitiveIntEncoder)))
    assert(JavaTypeInference.encoderFor(classOf[RecordWithBean]) ===
      JavaRecordEncoder(ClassTag(classOf[RecordWithBean]), Seq(
        recordField("id", StringEncoder),
        recordField("bean", beanEncoder))))
    assert(JavaTypeInference.encoderFor(classOf[BeanWithRecord]) ===
      JavaBeanEncoder(ClassTag(classOf[BeanWithRecord]), Seq(
        encoderField("address", expectedAddressEncoder))))
  }

  test("SPARK-55396: resolve generic record components bound by the enclosing record") {
    assert(JavaTypeInference.encoderFor(classOf[BoxHolder]) ===
      JavaRecordEncoder(ClassTag(classOf[BoxHolder]), Seq(
        recordField("stringBox", genericBoxEncoder(StringEncoder)),
        recordField("intBox", genericBoxEncoder(BoxedIntEncoder)),
        recordField("boxes", listEncoder(genericBoxEncoder(StringEncoder))))))
  }

  test("SPARK-55396: unsupported record classes") {
    checkError(
      exception = intercept[SparkRuntimeException](Encoders.record(classOf[String])),
      condition = "NOT_A_RECORD_CLASS",
      parameters = Map("className" -> "java.lang.String"))
    // Type parameters are unbound at the top level or when used as a raw type.
    Seq(classOf[GenericBox[_]], classOf[RawBoxHolder]).foreach { cls =>
      checkError(
        exception = intercept[SparkUnsupportedOperationException](Encoders.record(cls)),
        condition = "GENERIC_RECORD_NOT_SUPPORTED",
        parameters = Map("recordClass" -> classOf[GenericBox[_]].getName, "typeParams" -> "T"))
    }
    checkError(
      exception = intercept[SparkUnsupportedOperationException](
        Encoders.record(classOf[SelfReferencingRecord])),
      condition = "CIRCULAR_CLASS_REFERENCE",
      parameters = Map("t" -> s"'${classOf[SelfReferencingRecord]}'"))
  }
}
