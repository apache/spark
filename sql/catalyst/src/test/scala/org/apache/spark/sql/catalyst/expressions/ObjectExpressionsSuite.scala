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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolveTimeZone
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{SQLDate, SQLTimestamp}
import org.apache.spark.sql.internal.SQLConf
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

class ObjectExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("SPARK-16622: The returned value of the called method in Invoke can be null") {
    val inputRow = InternalRow.fromSeq(Seq((false, null)))
    val cls = classOf[Tuple2[Boolean, java.lang.Integer]]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    val invoke = Invoke(inputObject, "_2", IntegerType)
    checkEvaluationWithGeneratedMutableProjection(invoke, null, inputRow)
  }

  test("MapObjects should make copies of unsafe-backed data") {
    // test UnsafeRow-backed data
    val structEncoder = ExpressionEncoder[Array[Tuple2[java.lang.Integer, java.lang.Integer]]]
    val structInputRow = InternalRow.fromSeq(Seq(Array((1, 2), (3, 4))))
    val structExpected = new GenericArrayData(
      Array(InternalRow.fromSeq(Seq(1, 2)), InternalRow.fromSeq(Seq(3, 4))))
    checkEvaluationWithUnsafeProjection(
      structEncoder.serializer.head,
      structExpected,
      structInputRow,
      UnsafeProjection) // TODO(hvanhovell) revert this when SPARK-23587 is fixed

    // test UnsafeArray-backed data
    val arrayEncoder = ExpressionEncoder[Array[Array[Int]]]
    val arrayInputRow = InternalRow.fromSeq(Seq(Array(Array(1, 2), Array(3, 4))))
    val arrayExpected = new GenericArrayData(
      Array(new GenericArrayData(Array(1, 2)), new GenericArrayData(Array(3, 4))))
    checkEvaluationWithUnsafeProjection(
      arrayEncoder.serializer.head,
      arrayExpected,
      arrayInputRow,
      UnsafeProjection) // TODO(hvanhovell) revert this when SPARK-23587 is fixed

    // test UnsafeMap-backed data
    val mapEncoder = ExpressionEncoder[Array[Map[Int, Int]]]
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
      mapEncoder.serializer.head,
      mapExpected,
      mapInputRow,
      UnsafeProjection) // TODO(hvanhovell) revert this when SPARK-23587 is fixed
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
        "toJavaDate", ObjectType(classOf[SQLDate]), 77777, DateTimeUtils.toJavaDate(77777)),
      (DateTimeUtils.getClass, ObjectType(classOf[Timestamp]),
        "toJavaTimestamp", ObjectType(classOf[SQLTimestamp]),
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
      Map("nonexisting" -> Literal(1)))
    checkExceptionInExpression[Exception](initializeWithNonexistingMethod,
      InternalRow.fromSeq(Seq()),
      """A method named "nonexisting" is not declared in any enclosing class """ +
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
    evaluateWithGeneratedMutableProjection(initializeBean, InternalRow.fromSeq(Seq()))

    val initializeBean2 = InitializeJavaBean(
      Literal.fromObject(new TestBean),
      Map("setNonPrimitive" -> Literal("string")))
    evaluateWithoutCodegen(initializeBean2, InternalRow.fromSeq(Seq()))
    evaluateWithGeneratedMutableProjection(initializeBean2, InternalRow.fromSeq(Seq()))
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
    val serializer = new JavaSerializer(new SparkConf()).newInstance
    val resolver = ResolveTimeZone(new SQLConf)
    val expr = resolver.resolveTimeZones(serializer.deserialize(serializer.serialize(expression)))
    checkEvaluationWithoutCodegen(expr, expected, inputRow)
    checkEvaluationWithGeneratedMutableProjection(expr, expected, inputRow)
    if (GenerateUnsafeProjection.canSupport(expr.dataType)) {
      checkEvaluationWithUnsafeProjection(
        expr,
        expected,
        inputRow,
        UnsafeProjection) // TODO(hvanhovell) revert this when SPARK-23587 is fixed
    }
    checkEvaluationWithOptimization(expr, expected, inputRow)
  }

  test("SPARK-23594 GetExternalRowField should support interpreted execution") {
    val inputObject = BoundReference(0, ObjectType(classOf[Row]), nullable = true)
    val getRowField = GetExternalRowField(inputObject, index = 0, fieldName = "c0")
    Seq((Row(1), 1), (Row(3), 3)).foreach { case (input, expected) =>
      checkEvaluation(getRowField, expected, InternalRow.fromSeq(Seq(input)))
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
      val expected = serializer.newInstance().serialize(new Integer(1)).array()
      val encodeUsingSerializer = EncodeUsingSerializer(inputObject, useKryo)
      checkEvaluation(encodeUsingSerializer, expected, InternalRow.fromSeq(Seq(1)))
      checkEvaluation(encodeUsingSerializer, null, InternalRow.fromSeq(Seq(null)))
    }
  }

  test("SPARK-23587: MapObjects should support interpreted execution") {
    def testMapObjects(collection: Any, collectionCls: Class[_], inputType: DataType): Unit = {
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
          assert(result.asInstanceOf[java.util.List[_]].asScala.toSeq == expected)
        case s if classOf[Seq[_]].isAssignableFrom(s) =>
          assert(result.asInstanceOf[Seq[_]].toSeq == expected)
        case s if classOf[scala.collection.Set[_]].isAssignableFrom(s) =>
          assert(result.asInstanceOf[scala.collection.Set[_]] == expected.toSet)
      }
    }

    val customCollectionClasses = Seq(classOf[Seq[Int]], classOf[scala.collection.Set[Int]],
      classOf[java.util.List[Int]], classOf[java.util.AbstractList[Int]],
      classOf[java.util.AbstractSequentialList[Int]], classOf[java.util.Vector[Int]],
      classOf[java.util.Stack[Int]], null)

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
      val errMsg = intercept[RuntimeException] {
        testMapObjects(collection, classOf[scala.collection.Map[Int, Int]], inputType)
      }.getMessage()
      assert(errMsg.contains("`scala.collection.Map` is not supported by `MapObjects` " +
        "as resulting collection."))
    }
  }

  test("SPARK-23592: DecodeUsingSerializer should support interpreted execution") {
    val cls = classOf[java.lang.Integer]
    val inputObject = BoundReference(0, ObjectType(classOf[Array[Byte]]), nullable = true)
    val conf = new SparkConf()
    Seq(true, false).foreach { useKryo =>
      val serializer = if (useKryo) new KryoSerializer(conf) else new JavaSerializer(conf)
      val input = serializer.newInstance().serialize(new Integer(1)).array()
      val decodeUsingSerializer = DecodeUsingSerializer(inputObject, ClassTag(cls), useKryo)
      checkEvaluation(decodeUsingSerializer, new Integer(1), InternalRow.fromSeq(Seq(input)))
      checkEvaluation(decodeUsingSerializer, null, InternalRow.fromSeq(Seq(null)))
    }
  }
}

class TestBean extends Serializable {
  private var x: Int = 0

  def setX(i: Int): Unit = x = i
  def setNonPrimitive(i: AnyRef): Unit =
    assert(i != null, "this setter should not be called with null.")
}
