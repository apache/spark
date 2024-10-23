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

import java.sql.{Date, Timestamp}

import scala.collection.immutable
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Random

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection, ScroogeLikeExample}
import org.apache.spark.sql.catalyst.analysis.{ResolveTimeZone, SimpleAnalyzer, UnresolvedDeserializer}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.encoders._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData, IntervalUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class InvokeTargetClass extends Serializable {
  def filterInt(e: Any): Any = e.asInstanceOf[Int] > 0
  def filterPrimitiveInt(e: Int): Boolean = e > 0
  def binOp(e1: Int, e2: Double): Double = e1 + e2
}

class InvokeTargetSubClass extends InvokeTargetClass {
  override def binOp(e1: Int, e2: Double): Double = e1 - e2
}

// Tests for NewInstance
class Outer extends Serializable {
  class Inner(val value: Int) {
    override def hashCode(): Int = super.hashCode()
    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[Inner]) {
        value == other.asInstanceOf[Inner].value
      } else {
        false
      }
    }
  }
}

class ObjectExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("SPARK-16622: The returned value of the called method in Invoke can be null") {
    val inputRow = InternalRow.fromSeq(Seq((false, null)))
    val cls = classOf[Tuple2[Boolean, java.lang.Integer]]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val invoke = Invoke(inputObject, "_2", IntegerType)
    checkEvaluationWithMutableProjection(invoke, null, inputRow)
  }

  test("SPARK-44525: Invoke could not find method") {
    val inputRow = InternalRow(new Object)
    val inputObject = BoundReference(0, ObjectType(classOf[Object]), nullable = false)

    checkError(
      exception = intercept[SparkException] {
        Invoke(inputObject, "zeroArgNotExistMethod", IntegerType).eval(inputRow)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" ->
        ("Couldn't find method zeroArgNotExistMethod with arguments " +
          "() on class java.lang.Object.")
      )
    )

    checkError(
      exception = intercept[SparkException] {
        Invoke(
          inputObject,
          "oneArgNotExistMethod",
          IntegerType,
          Seq(Literal.fromObject(UTF8String.fromString("dummyInputString"))),
          Seq(StringType)).eval(inputRow)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map("message" ->
        ("Couldn't find method oneArgNotExistMethod with arguments " +
          "(class org.apache.spark.unsafe.types.UTF8String) on class java.lang.Object.")
      )
    )
  }

  test("MapObjects should make copies of unsafe-backed data") {
    // test UnsafeRow-backed data
    val structEncoder = ExpressionEncoder[Array[Tuple2[java.lang.Integer, java.lang.Integer]]]()
    val structInputRow = InternalRow.fromSeq(Seq(Array((1, 2), (3, 4))))
    val structExpected = new GenericArrayData(
      Array(InternalRow.fromSeq(Seq(1, 2)), InternalRow.fromSeq(Seq(3, 4))))
    checkEvaluationWithUnsafeProjection(
      structEncoder.serializer.head, structExpected, structInputRow)

    // test UnsafeArray-backed data
    val arrayEncoder = ExpressionEncoder[Array[Array[Int]]]()
    val arrayInputRow = InternalRow.fromSeq(Seq(Array(Array(1, 2), Array(3, 4))))
    val arrayExpected = new GenericArrayData(
      Array(new GenericArrayData(Array(1, 2)), new GenericArrayData(Array(3, 4))))
    checkEvaluationWithUnsafeProjection(
      arrayEncoder.serializer.head, arrayExpected, arrayInputRow)

    // test UnsafeMap-backed data
    val mapEncoder = ExpressionEncoder[Array[Map[Int, Int]]]()
    val mapInputRow = InternalRow.fromSeq(Seq(Array(
      Map(1 -> 100, 2 -> 200), Map(3 -> 300, 4 -> 400))))
    val mapExpected = new GenericArrayData(Seq(
      new ArrayBasedMapData(
        new GenericArrayData(Array(1, 2)),
        new GenericArrayData(Array(100, 200))),
      new ArrayBasedMapData(
        new GenericArrayData(Array(3, 4)),
        new GenericArrayData(Array(300, 400)))))
    checkEvaluationWithUnsafeProjection(
      mapEncoder.serializer.head, mapExpected, mapInputRow)
  }

  test("SPARK-23582: StaticInvoke should support interpreted execution") {
    Seq((classOf[java.lang.Boolean], "true", true),
      (classOf[java.lang.Byte], "1", 1.toByte),
      (classOf[java.lang.Short], "257", 257.toShort),
      (classOf[java.lang.Integer], "12345", 12345),
      (classOf[java.lang.Long], "12345678", 12345678.toLong),
      (classOf[java.lang.Float], "12.34", 12.34.toFloat),
      (classOf[java.lang.Double], "1.2345678", 1.2345678)
    ).foreach { case (cls, arg, expected) =>
      checkObjectExprEvaluation(StaticInvoke(cls, ObjectType(cls), "valueOf",
        Seq(BoundReference(0, ObjectType(classOf[java.lang.String]), true))),
        expected, InternalRow.fromSeq(Seq(arg)))
    }

    // Return null when null argument is passed with propagateNull = true
    val stringCls = classOf[java.lang.String]
    checkObjectExprEvaluation(StaticInvoke(stringCls, ObjectType(stringCls), "valueOf",
      Seq(BoundReference(0, ObjectType(classOf[Object]), true)), propagateNull = true),
      null, InternalRow.fromSeq(Seq(null)))
    checkObjectExprEvaluation(StaticInvoke(stringCls, ObjectType(stringCls), "valueOf",
      Seq(BoundReference(0, ObjectType(classOf[Object]), true)), propagateNull = false),
      "null", InternalRow.fromSeq(Seq(null)))

    // test no argument
    val clCls = classOf[java.lang.ClassLoader]
    checkObjectExprEvaluation(StaticInvoke(clCls, ObjectType(clCls), "getSystemClassLoader", Nil),
      ClassLoader.getSystemClassLoader, InternalRow.empty)
    // test more than one argument
    val intCls = classOf[java.lang.Integer]
    checkObjectExprEvaluation(StaticInvoke(intCls, ObjectType(intCls), "compare",
      Seq(BoundReference(0, IntegerType, false), BoundReference(1, IntegerType, false))),
      0, InternalRow.fromSeq(Seq(7, 7)))

    Seq((DateTimeUtils.getClass, TimestampType, "fromJavaTimestamp", ObjectType(classOf[Timestamp]),
      new Timestamp(77777), DateTimeUtils.fromJavaTimestamp(new Timestamp(77777))),
      (DateTimeUtils.getClass, DateType, "fromJavaDate", ObjectType(classOf[Date]),
        new Date(88888888), DateTimeUtils.fromJavaDate(new Date(88888888))),
      (classOf[UTF8String], StringType, "fromString", ObjectType(classOf[String]),
        "abc", UTF8String.fromString("abc")),
      (Decimal.getClass, DecimalType(38, 0), "fromDecimal", ObjectType(classOf[Any]),
        BigInt(88888888), Decimal.fromDecimal(BigInt(88888888))),
      (Decimal.getClass, DecimalType.SYSTEM_DEFAULT,
        "apply", ObjectType(classOf[java.math.BigInteger]),
        new java.math.BigInteger("88888888"), Decimal.apply(new java.math.BigInteger("88888888"))),
      (classOf[ArrayData], ArrayType(IntegerType), "toArrayData", ObjectType(classOf[Any]),
        Array[Int](1, 2, 3), ArrayData.toArrayData(Array[Int](1, 2, 3))),
      (classOf[UnsafeArrayData], ArrayType(IntegerType, false),
        "fromPrimitiveArray", ObjectType(classOf[Array[Int]]),
        Array[Int](1, 2, 3), UnsafeArrayData.fromPrimitiveArray(Array[Int](1, 2, 3))),
      (DateTimeUtils.getClass, ObjectType(classOf[Date]),
        "toJavaDate", ObjectType(classOf[Int]), 77777,
        DateTimeUtils.toJavaDate(77777)),
      (DateTimeUtils.getClass, ObjectType(classOf[Timestamp]),
        "toJavaTimestamp", ObjectType(classOf[Long]),
        88888888.toLong, DateTimeUtils.toJavaTimestamp(88888888))
    ).foreach { case (cls, dataType, methodName, argType, arg, expected) =>
      checkObjectExprEvaluation(StaticInvoke(cls, dataType, methodName,
        Seq(BoundReference(0, argType, true))), expected, InternalRow.fromSeq(Seq(arg)))
    }
  }

  test("SPARK-23583: Invoke should support interpreted execution") {
    val targetObject = new InvokeTargetClass
    val funcClass = classOf[InvokeTargetClass]
    val funcObj = Literal.create(targetObject, ObjectType(funcClass))
    val targetSubObject = new InvokeTargetSubClass
    val funcSubObj = Literal.create(targetSubObject, ObjectType(classOf[InvokeTargetSubClass]))
    val funcNullObj = Literal.create(null, ObjectType(funcClass))

    val inputInt = Seq(BoundReference(0, ObjectType(classOf[Any]), true))
    val inputPrimitiveInt = Seq(BoundReference(0, IntegerType, false))
    val inputSum = Seq(BoundReference(0, IntegerType, false), BoundReference(1, DoubleType, false))

    checkObjectExprEvaluation(
      Invoke(funcObj, "filterInt", ObjectType(classOf[Any]), inputInt),
      java.lang.Boolean.valueOf(true), InternalRow.fromSeq(Seq(Integer.valueOf(1))))

    checkObjectExprEvaluation(
      Invoke(funcObj, "filterPrimitiveInt", BooleanType, inputPrimitiveInt),
      false, InternalRow.fromSeq(Seq(-1)))

    checkObjectExprEvaluation(
      Invoke(funcObj, "filterInt", ObjectType(classOf[Any]), inputInt),
      null, InternalRow.fromSeq(Seq(null)))

    checkObjectExprEvaluation(
      Invoke(funcNullObj, "filterInt", ObjectType(classOf[Any]), inputInt),
      null, InternalRow.fromSeq(Seq(Integer.valueOf(1))))

    checkObjectExprEvaluation(
      Invoke(funcObj, "binOp", DoubleType, inputSum), 1.25, InternalRow.apply(1, 0.25))

    checkObjectExprEvaluation(
      Invoke(funcSubObj, "binOp", DoubleType, inputSum), 0.75, InternalRow.apply(1, 0.25))
  }

  test("SPARK-23593: InitializeJavaBean should support interpreted execution") {
    val list = new java.util.LinkedList[Int]()
    list.add(1)

    val initializeBean = InitializeJavaBean(Literal.fromObject(new java.util.LinkedList[Int]),
      Map("add" -> Literal(1)))
    checkEvaluation(initializeBean, list, InternalRow.fromSeq(Seq()))

    val initializeWithNonexistingMethod = InitializeJavaBean(
      Literal.fromObject(new java.util.LinkedList[Int]),
      Map("nonexistent" -> Literal(1)))
    checkExceptionInExpression[Exception](initializeWithNonexistingMethod,
      """A method named "nonexistent" is not declared in any enclosing class """ +
        "nor any supertype")

    val initializeWithWrongParamType = InitializeJavaBean(
      Literal.fromObject(new TestBean),
      Map("setX" -> Literal("1")))
    intercept[Exception] {
      evaluateWithoutCodegen(initializeWithWrongParamType, InternalRow.fromSeq(Seq()))
    }.getMessage.contains(
      """A method named "setX" is not declared in any enclosing class """ +
        "nor any supertype")
  }

  test("InitializeJavaBean doesn't call setters if input in null") {
    val initializeBean = InitializeJavaBean(
      Literal.fromObject(new TestBean),
      Map("setNonPrimitive" -> Literal(null)))
    evaluateWithoutCodegen(initializeBean, InternalRow.fromSeq(Seq()))
    evaluateWithMutableProjection(initializeBean, InternalRow.fromSeq(Seq()))

    val initializeBean2 = InitializeJavaBean(
      Literal.fromObject(new TestBean),
      Map("setNonPrimitive" -> Literal("string")))
    evaluateWithoutCodegen(initializeBean2, InternalRow.fromSeq(Seq()))
    evaluateWithMutableProjection(initializeBean2, InternalRow.fromSeq(Seq()))
  }

  test("SPARK-23585: UnwrapOption should support interpreted execution") {
    val cls = classOf[Option[Int]]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val unwrapObject = UnwrapOption(IntegerType, inputObject)
    Seq((Some(1), 1), (None, null), (null, null)).foreach { case (input, expected) =>
      checkEvaluation(unwrapObject, expected, InternalRow.fromSeq(Seq(input)))
    }
  }

  test("SPARK-23586: WrapOption should support interpreted execution") {
    val cls = ObjectType(classOf[java.lang.Integer])
    val inputObject = BoundReference(0, cls, nullable = true)
    val wrapObject = WrapOption(inputObject, cls)
    Seq((1, Some(1)), (null, None)).foreach { case (input, expected) =>
      checkEvaluation(wrapObject, expected, InternalRow.fromSeq(Seq(input)))
    }
  }

  test("SPARK-23590: CreateExternalRow should support interpreted execution") {
    val schema = new StructType().add("a", IntegerType).add("b", StringType)
    val createExternalRow = CreateExternalRow(Seq(Literal(1), Literal("x")), schema)
    checkEvaluation(createExternalRow, Row.fromSeq(Seq(1, "x")), InternalRow.fromSeq(Seq()))
  }

  // by scala values instead of catalyst values.
  private def checkObjectExprEvaluation(
      expression: => Expression, expected: Any, inputRow: InternalRow = EmptyRow): Unit = {
    val serializer = new JavaSerializer(new SparkConf()).newInstance()
    val resolver = ResolveTimeZone
    val expr = resolver.resolveTimeZones(serializer.deserialize(serializer.serialize(expression)))
    checkEvaluationWithoutCodegen(expr, expected, inputRow)
    checkEvaluationWithMutableProjection(expr, expected, inputRow)
    if (GenerateUnsafeProjection.canSupport(expr.dataType)) {
      checkEvaluationWithUnsafeProjection(
        expr,
        expected,
        inputRow)
    }
    checkEvaluationWithOptimization(expr, expected, inputRow)
  }

  test("SPARK-23594 GetExternalRowField should support interpreted execution") {
    val inputObject = BoundReference(0, ObjectType(classOf[Row]), nullable = true)
    val getRowField = GetExternalRowField(inputObject, index = 0, fieldName = "c0")
    Seq((Row(1), 1), (Row(3), 3)).foreach { case (input, expected) =>
      checkObjectExprEvaluation(getRowField, expected, InternalRow.fromSeq(Seq(input)))
    }

    // If an input row or a field are null, a runtime exception will be thrown
    checkExceptionInExpression[RuntimeException](
      getRowField,
      InternalRow.fromSeq(Seq(null)),
      "The input external row cannot be null.")
    checkExceptionInExpression[RuntimeException](
      getRowField,
      InternalRow.fromSeq(Seq(Row(null))),
      "The 0th field 'c0' of input row cannot be null.")
  }

  test("SPARK-23591: EncodeUsingSerializer should support interpreted execution") {
    val cls = ObjectType(classOf[java.lang.Integer])
    val inputObject = BoundReference(0, cls, nullable = true)
    val conf = new SparkConf()
    Seq(true, false).foreach { useKryo =>
      val serializer = if (useKryo) new KryoSerializer(conf) else new JavaSerializer(conf)
      val expected = serializer.newInstance().serialize(Integer.valueOf(1)).array()
      val encodeUsingSerializer = EncodeUsingSerializer(inputObject, useKryo)
      checkEvaluation(encodeUsingSerializer, expected, InternalRow.fromSeq(Seq(1)))
      checkEvaluation(encodeUsingSerializer, null, InternalRow.fromSeq(Seq(null)))
    }
  }

  test("SPARK-23587: MapObjects should support interpreted execution") {
    def testMapObjects[T](collection: Any, collectionCls: Class[T], inputType: DataType): Unit = {
      val function = (lambda: Expression) => Add(lambda, Literal(1))
      val elementType = IntegerType
      val expected = Seq(2, 3, 4)

      val inputObject = BoundReference(0, inputType, nullable = true)
      val optClass = Option(collectionCls)
      val mapObj = MapObjects(function, inputObject, elementType, true, optClass)
      val row = InternalRow.fromSeq(Seq(collection))
      val result = mapObj.eval(row)

      collectionCls match {
        case null =>
          assert(result.asInstanceOf[ArrayData].array.toSeq == expected)
        case l if classOf[java.util.List[_]].isAssignableFrom(l) =>
          assert(result.asInstanceOf[java.util.List[_]].asScala == expected)
        case s if classOf[java.util.Set[_]].isAssignableFrom(s) =>
          assert(result.asInstanceOf[java.util.Set[_]].asScala == expected.toSet)
        case a if classOf[mutable.ArraySeq[Int]].isAssignableFrom(a) =>
          assert(result == mutable.ArraySeq.make[Int](expected.toArray))
        case a if classOf[immutable.ArraySeq[Int]].isAssignableFrom(a) =>
          assert(result.isInstanceOf[immutable.ArraySeq[_]])
          assert(result == immutable.ArraySeq.unsafeWrapArray[Int](expected.toArray))
        case s if classOf[Seq[_]].isAssignableFrom(s) =>
          assert(result.asInstanceOf[Seq[_]] == expected)
        case s if classOf[scala.collection.Set[_]].isAssignableFrom(s) =>
          assert(result.asInstanceOf[scala.collection.Set[_]] == expected.toSet)
      }
    }

    val customCollectionClasses = Seq(
      classOf[mutable.ArraySeq[Int]], classOf[immutable.ArraySeq[Int]],
      classOf[Seq[Int]], classOf[scala.collection.Set[Int]],
      classOf[java.util.List[Int]], classOf[java.util.AbstractList[Int]],
      classOf[java.util.AbstractSequentialList[Int]], classOf[java.util.Vector[Int]],
      classOf[java.util.Stack[Int]], null,
      classOf[java.util.Set[Int]])

    val list = new java.util.ArrayList[Int]()
    list.add(1)
    list.add(2)
    list.add(3)
    val arrayData = new GenericArrayData(Array(1, 2, 3))
    val vector = new java.util.Vector[Int]()
    vector.add(1)
    vector.add(2)
    vector.add(3)
    val stack = new java.util.Stack[Int]()
    stack.add(1)
    stack.add(2)
    stack.add(3)

    Seq(
      (Seq(1, 2, 3), ObjectType(classOf[mutable.ArraySeq[Int]])),
      (Seq(1, 2, 3), ObjectType(classOf[immutable.ArraySeq[Int]])),
      (Seq(1, 2, 3), ObjectType(classOf[Seq[Int]])),
      (Array(1, 2, 3), ObjectType(classOf[Array[Int]])),
      (Seq(1, 2, 3), ObjectType(classOf[Object])),
      (Array(1, 2, 3), ObjectType(classOf[Object])),
      (list, ObjectType(classOf[java.util.List[Int]])),
      (vector, ObjectType(classOf[java.util.Vector[Int]])),
      (stack, ObjectType(classOf[java.util.Stack[Int]])),
      (arrayData, ArrayType(IntegerType))
    ).foreach { case (collection, inputType) =>
      customCollectionClasses.foreach(testMapObjects(collection, _, inputType))

      // Unsupported custom collection class
      checkError(
        exception = intercept[SparkRuntimeException] {
          testMapObjects(collection, classOf[scala.collection.Map[Int, Int]], inputType)
        },
        condition = "CLASS_UNSUPPORTED_BY_MAP_OBJECTS",
        parameters = Map("cls" -> "scala.collection.Map"))
    }
  }

  test("SPARK-23592: DecodeUsingSerializer should support interpreted execution") {
    val cls = classOf[java.lang.Integer]
    val inputObject = BoundReference(0, ObjectType(classOf[Array[Byte]]), nullable = true)
    val conf = new SparkConf()
    Seq(true, false).foreach { useKryo =>
      val serializer = if (useKryo) new KryoSerializer(conf) else new JavaSerializer(conf)
      val input = serializer.newInstance().serialize(Integer.valueOf(1)).array()
      val decodeUsingSerializer = DecodeUsingSerializer(inputObject, ClassTag(cls), useKryo)
      checkEvaluation(decodeUsingSerializer, Integer.valueOf(1), InternalRow.fromSeq(Seq(input)))
      checkEvaluation(decodeUsingSerializer, null, InternalRow.fromSeq(Seq(null)))
    }
  }

  test("SPARK-23584 NewInstance should support interpreted execution") {
    // Normal case test
    val newInst1 = NewInstance(
      cls = classOf[GenericArrayData],
      arguments = Literal.fromObject(List(1, 2, 3)) :: Nil,
      inputTypes = Nil,
      propagateNull = false,
      dataType = ArrayType(IntegerType),
      outerPointer = None)
    checkObjectExprEvaluation(newInst1, new GenericArrayData(List(1, 2, 3)))

    // Inner class case test
    val outerObj = new Outer()
    val newInst2 = NewInstance(
      cls = classOf[outerObj.Inner],
      arguments = Literal(1) :: Nil,
      inputTypes = Nil,
      propagateNull = false,
      dataType = ObjectType(classOf[outerObj.Inner]),
      outerPointer = Some(() => outerObj))
    checkObjectExprEvaluation(newInst2, new outerObj.Inner(1))

    // SPARK-8288: A class with only a companion object constructor
    val newInst3 = NewInstance(
      cls = classOf[ScroogeLikeExample],
      arguments = Literal(1) :: Nil,
      inputTypes = Nil,
      propagateNull = false,
      dataType = ObjectType(classOf[ScroogeLikeExample]),
      outerPointer = None)
    checkObjectExprEvaluation(newInst3, ScroogeLikeExample(1))
  }

  test("LambdaVariable should support interpreted execution") {
    def genSchema(dt: DataType): Seq[StructType] = {
      Seq(StructType(StructField("col_1", dt, nullable = false) :: Nil),
        StructType(StructField("col_1", dt, nullable = true) :: Nil))
    }

    val elementTypes = Seq(BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
      DoubleType, DecimalType.USER_DEFAULT, StringType, BinaryType, DateType, TimestampType,
      CalendarIntervalType, new ExamplePointUDT())
    val arrayTypes = elementTypes.flatMap { elementType =>
      Seq(ArrayType(elementType, containsNull = false), ArrayType(elementType, containsNull = true))
    }
    val mapTypes = elementTypes.flatMap { elementType =>
      Seq(MapType(elementType, elementType, false), MapType(elementType, elementType, true))
    }
    val structTypes = elementTypes.flatMap { elementType =>
      Seq(StructType(StructField("col1", elementType, false) :: Nil),
        StructType(StructField("col1", elementType, true) :: Nil))
    }

    val testTypes = elementTypes ++ arrayTypes ++ mapTypes ++ structTypes
    val random = new Random(100)
    testTypes.foreach { dt =>
      genSchema(dt).map { schema =>
        val row = RandomDataGenerator.randomRow(random, schema)
        val toRow = ExpressionEncoder(schema).createSerializer()
        val internalRow = toRow(row)
        val lambda = LambdaVariable("dummy", schema(0).dataType, schema(0).nullable, id = 0)
        checkEvaluationWithoutCodegen(lambda, internalRow.get(0, schema(0).dataType), internalRow)
      }
    }
  }

  implicit private def mapIntStrEncoder: ExpressionEncoder[Map[Int, String]] =
    ExpressionEncoder[Map[Int, String]]()

  test("SPARK-23588 CatalystToExternalMap should support interpreted execution") {
    // To get a resolved `CatalystToExternalMap` expression, we build a deserializer plan
    // with dummy input, resolve the plan by the analyzer, and replace the dummy input
    // with a literal for tests.
    val unresolvedDeser = UnresolvedDeserializer(encoderFor[Map[Int, String]].deserializer)
    val dummyInputPlan = LocalRelation(Symbol("value").map(MapType(IntegerType, StringType)))
    val plan = Project(Alias(unresolvedDeser, "none")() :: Nil, dummyInputPlan)

    val analyzedPlan = SimpleAnalyzer.execute(plan)
    val Alias(toMapExpr: CatalystToExternalMap, _) = analyzedPlan.expressions.head

    // Replaces the dummy input with a literal for tests here
    val data = Map[Int, String](0 -> "v0", 1 -> "v1", 2 -> null, 3 -> "v3")
    val deserializer = toMapExpr.copy(inputData = Literal.create(data))
    checkObjectExprEvaluation(deserializer, expected = data)
  }

  test("SPARK-23595 ValidateExternalType should support interpreted execution") {
    val inputObject = BoundReference(0, ObjectType(classOf[Row]), nullable = true)
    Seq(
      (true, BooleanType),
      (2.toByte, ByteType),
      (5.toShort, ShortType),
      (23, IntegerType),
      (61L, LongType),
      (1.0f, FloatType),
      (10.0, DoubleType),
      ("abcd".getBytes, BinaryType),
      ("abcd", StringType),
      (BigDecimal.valueOf(10), DecimalType.IntDecimal),
      (IntervalUtils.stringToInterval(UTF8String.fromString("interval 3 day")),
        CalendarIntervalType),
      (java.math.BigDecimal.valueOf(10), DecimalType.BigIntDecimal),
      (Array(3, 2, 1), ArrayType(IntegerType))
    ).foreach { case (input, dt) =>
      val enc = RowEncoder.encoderForDataType(dt, lenient = false)
      val validateType = ValidateExternalType(
        GetExternalRowField(inputObject, index = 0, fieldName = "c0"),
        dt,
        EncoderUtils.lenientExternalDataTypeFor(enc))
      checkObjectExprEvaluation(validateType, input, InternalRow.fromSeq(Seq(Row(input))))
    }

    checkExceptionInExpression[SparkRuntimeException](
      ValidateExternalType(
        GetExternalRowField(inputObject, index = 0, fieldName = "c0"),
        DoubleType,
        DoubleType),
      InternalRow.fromSeq(Seq(Row(1))),
      "The external type java.lang.Integer is not valid for the type \"DOUBLE\"")
  }

  test("SPARK-49044 ValidateExternalType should return child in error") {
    val inputObject = BoundReference(0, ObjectType(classOf[Row]), nullable = true)
    Seq(
      (true, BooleanType),
      (2.toByte, ByteType),
      (5.toShort, ShortType),
      (23, IntegerType),
      (61L, LongType),
      (1.0f, FloatType),
      (10.0, DoubleType),
      ("abcd".getBytes, BinaryType),
      ("abcd", StringType),
      (BigDecimal.valueOf(10), DecimalType.IntDecimal),
      (IntervalUtils.stringToInterval(UTF8String.fromString("interval 3 day")),
        CalendarIntervalType),
      (java.math.BigDecimal.valueOf(10), DecimalType.BigIntDecimal),
      (Array(3, 2, 1), ArrayType(IntegerType))
    ).foreach { case (input, dt) =>
      val enc = RowEncoder.encoderForDataType(dt, lenient = false)
      val validateType = ValidateExternalType(
        GetExternalRowField(inputObject, index = 0, fieldName = "c0"),
        dt,
        EncoderUtils.lenientExternalDataTypeFor(enc))
      checkObjectExprEvaluation(validateType, input, InternalRow.fromSeq(Seq(Row(input))))
    }

    checkErrorInExpression[SparkRuntimeException](
      expression = ValidateExternalType(
        GetExternalRowField(inputObject, index = 0, fieldName = "c0"),
        DoubleType,
        DoubleType),
      inputRow = InternalRow.fromSeq(Seq(Row(1))),
      condition = "INVALID_EXTERNAL_TYPE",
      parameters = Map[String, String](
        "externalType" -> "java.lang.Integer",
        "type" -> "\"DOUBLE\"",
        "expr" -> ("\"getexternalrowfield(input[0, org.apache.spark.sql.Row, true], " +
          "0, c0)\"")
      )
    )
  }

  private def javaMapSerializerFor(
      keyClazz: Class[_],
      valueClazz: Class[_])(inputObject: Expression): Expression = {

    def kvSerializerFor(inputObject: Expression, clazz: Class[_]): Expression = clazz match {
      case c if c == classOf[java.lang.Integer] =>
        Invoke(inputObject, "intValue", IntegerType)
      case c if c == classOf[java.lang.String] =>
        StaticInvoke(
          classOf[UTF8String],
          StringType,
          "fromString",
          inputObject :: Nil,
          returnNullable = false)
    }

    ExternalMapToCatalyst(
      inputObject,
      ObjectType(keyClazz),
      kvSerializerFor(_, keyClazz),
      keyNullable = true,
      ObjectType(valueClazz),
      kvSerializerFor(_, valueClazz),
      valueNullable = true
    )
  }

  private def scalaMapSerializerFor[T: TypeTag, U: TypeTag](inputObject: Expression): Expression = {
    val keyEnc = ScalaReflection.encoderFor[T]
    val valueEnc = ScalaReflection.encoderFor[U]

    def kvSerializerFor(enc: AgnosticEncoder[_])(inputObject: Expression): Expression = enc match {
      case AgnosticEncoders.BoxedIntEncoder =>
        Invoke(inputObject, "intValue", IntegerType)
      case AgnosticEncoders.StringEncoder =>
        StaticInvoke(
          classOf[UTF8String],
          StringType,
          "fromString",
          inputObject :: Nil,
          returnNullable = false)
       case _ =>
         inputObject
    }

    ExternalMapToCatalyst(
      inputObject,
      EncoderUtils.externalDataTypeFor(keyEnc),
      kvSerializerFor(keyEnc),
      keyNullable = keyEnc.nullable,
      EncoderUtils.externalDataTypeFor(valueEnc),
      kvSerializerFor(valueEnc),
      valueNullable = valueEnc.nullable
    )
  }

  test("SPARK-23589 ExternalMapToCatalyst should support interpreted execution") {
    // Simple test
    val scalaMap = scala.collection.Map[Int, String](0 -> "v0", 1 -> "v1", 2 -> null, 3 -> "v3")
    val javaMap = new java.util.HashMap[java.lang.Integer, java.lang.String]() {
      {
        put(0, "v0")
        put(1, "v1")
        put(2, null)
        put(3, "v3")
      }
    }
    val expected = CatalystTypeConverters.convertToCatalyst(scalaMap)

    // Java Map
    val serializer1 = javaMapSerializerFor(classOf[java.lang.Integer], classOf[java.lang.String])(
      Literal.fromObject(javaMap))
    checkEvaluation(serializer1, expected)

    // Scala Map
    val serializer2 = scalaMapSerializerFor[Int, String](Literal.fromObject(scalaMap))
    checkEvaluation(serializer2, expected)

    // NULL key test
    val scalaMapHasNullKey = scala.collection.Map[java.lang.Integer, String](
      null.asInstanceOf[java.lang.Integer] -> "v0", java.lang.Integer.valueOf(1) -> "v1")

    val javaMapHasNullKey = new java.util.HashMap[java.lang.Integer, java.lang.String]() {
      {
        put(null, "v0")
        put(1, "v1")
      }
    }

    // Java Map
    val serializer3 =
      javaMapSerializerFor(classOf[java.lang.Integer], classOf[java.lang.String])(
        Literal.fromObject(javaMapHasNullKey))
    checkErrorInExpression[SparkRuntimeException](
      serializer3, EmptyRow, "NULL_MAP_KEY", Map[String, String]())

    // Scala Map
    val serializer4 = scalaMapSerializerFor[java.lang.Integer, String](
      Literal.fromObject(scalaMapHasNullKey))

    checkErrorInExpression[SparkRuntimeException](
      serializer4, EmptyRow, "NULL_MAP_KEY", Map[String, String]())
  }

  test("SPARK-35244: invoke should throw the original exception") {
    val strClsType = ObjectType(classOf[String])
    checkExceptionInExpression[StringIndexOutOfBoundsException](
      Invoke(Literal("a", strClsType), "substring", strClsType, Seq(Literal(3))), "")

    val mathCls = classOf[Math]
    checkExceptionInExpression[ArithmeticException](
      StaticInvoke(mathCls, IntegerType, "addExact", Seq(Literal(Int.MaxValue), Literal(1))), "")
  }

  test("SPARK-35278: invoke should find method with correct number of parameters") {
    val strClsType = ObjectType(classOf[String])
    checkExceptionInExpression[StringIndexOutOfBoundsException](
      Invoke(Literal("a", strClsType), "substring", strClsType, Seq(Literal(3))), "")

    checkObjectExprEvaluation(
      Invoke(Literal("a", strClsType), "substring", strClsType, Seq(Literal(0))), "a")

    checkExceptionInExpression[StringIndexOutOfBoundsException](
      Invoke(Literal("a", strClsType), "substring", strClsType, Seq(Literal(0), Literal(3))), "")

    checkObjectExprEvaluation(
      Invoke(Literal("a", strClsType), "substring", strClsType, Seq(Literal(0), Literal(1))), "a")
  }

  test("SPARK-35278: invoke should correctly invoke override method") {
    val clsType = ObjectType(classOf[ConcreteClass])
    val obj = new ConcreteClass

    val input = (1, 2)
    checkObjectExprEvaluation(
      Invoke(Literal(obj, clsType), "testFunc", IntegerType,
        Seq(Literal(input, ObjectType(input.getClass)))), 2)
  }

  test("SPARK-35288: static invoke should find method without exact param type match") {
    val input = (1, 2)

    checkObjectExprEvaluation(
      StaticInvoke(TestStaticInvoke.getClass, IntegerType, "func",
        Seq(Literal(input, ObjectType(input.getClass)))), 3)

    checkObjectExprEvaluation(
      StaticInvoke(TestStaticInvoke.getClass, IntegerType, "func",
        Seq(Literal(1, IntegerType))), -1)
  }

  test("SPARK-35281: StaticInvoke shouldn't box primitive when result is nullable") {
    val ctx = new CodegenContext
    val arguments = Seq(Literal(0), Literal(1))
    val genCode = StaticInvoke(TestFun.getClass, IntegerType, "foo", arguments).genCode(ctx)
    assert(!genCode.code.toString.contains("boxedResult"))
  }

  test("StaticInvoke call return `any` method") {
    val cls = TestStaticInvokeReturnAny.getClass
    Seq((0, IntegerType, true), (1, IntegerType, true), (2, IntegerType, false)).foreach {
      case (arg, argDataType, returnNullable) =>
        val dataType = arg match {
          case 0 => ObjectType(classOf[java.lang.Integer])
          case 1 => ShortType
          case 2 => ObjectType(classOf[java.lang.Long])
        }
        val arguments = Seq(Literal(arg, argDataType))
        val inputTypes = Seq(IntegerType)
        val expected = arg match {
          case 0 => java.lang.Integer.valueOf(1)
          case 1 => 0.toShort
          case 2 => java.lang.Long.valueOf(2)
        }
        val inputRow = InternalRow.fromSeq(Seq(arg))
        checkObjectExprEvaluation(
          StaticInvoke(cls, dataType, "func", arguments, inputTypes,
            returnNullable = returnNullable),
          expected,
          inputRow)
    }
  }
}

class TestBean extends Serializable {
  private var x: Int = 0

  def setX(i: Int): Unit = x = i

  def setNonPrimitive(i: AnyRef): Unit =
    assert(i != null, "this setter should not be called with null.")
}

object TestStaticInvoke {
  def func(param: Any): Int = param match {
    case pair: Tuple2[_, _] =>
      pair.asInstanceOf[Tuple2[Int, Int]]._1 + pair.asInstanceOf[Tuple2[Int, Int]]._2
    case _ => -1
  }
}

abstract class BaseClass[T] {
  def testFunc(param: T): Int
}

class ConcreteClass extends BaseClass[Product] with Serializable {
  override def testFunc(param: Product): Int = param match {
    case _: Tuple2[_, _] => 2
    case _: Tuple3[_, _, _] => 3
    case _ => 4
  }
}

case object TestFun {
  def foo(left: Int, right: Int): Int = left + right
}

object TestStaticInvokeReturnAny {
  def func(input: Int): Any = input match {
    case 0 => java.lang.Integer.valueOf(1)
    case 1 => 0.toShort
    case 2 => java.lang.Long.valueOf(2)
  }
}
