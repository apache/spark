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

import org.apache.spark.sql.catalyst.CatalystTypeConverters.{createToCatalystConverter, createToScalaConverter => catalystCreateToScalaConverter, isPrimitive}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.trees.TreePattern.{SCALA_UDF, TreePattern}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, DataType}
import org.apache.spark.util.Utils

/**
 * User-defined function.
 * @param function  The user defined scala function to run.
 *                  Note that if you use primitive parameters, you are not able to check if it is
 *                  null or not, and the UDF will return null for you if the primitive input is
 *                  null. Use boxed type or [[Option]] if you wanna do the null-handling yourself.
 * @param dataType  Return type of function.
 * @param children  The input expressions of this UDF.
 * @param inputEncoders ExpressionEncoder for each input parameters. For a input parameter which
 *                      serialized as struct will use encoder instead of CatalystTypeConverters to
 *                      convert internal value to Scala value.
 * @param outputEncoder ExpressionEncoder for the return type of function. It's only defined when
 *                      this is a typed Scala UDF.
 * @param udfName  The user-specified name of this UDF.
 * @param nullable  True if the UDF can return null value.
 * @param udfDeterministic  True if the UDF is deterministic. Deterministic UDF returns same result
 *                          each time it is invoked with a particular input.
 */
case class ScalaUDF(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
    outputEncoder: Option[ExpressionEncoder[_]] = None,
    udfName: Option[String] = None,
    nullable: Boolean = true,
    udfDeterministic: Boolean = true)
  extends Expression with NonSQLExpression with UserDefinedExpression {

  override lazy val deterministic: Boolean = udfDeterministic && children.forall(_.deterministic)

  final override val nodePatterns: Seq[TreePattern] = Seq(SCALA_UDF)

  override def toString: String = s"$name(${children.mkString(", ")})"

  override def name: String = udfName.getOrElse("UDF")

  override lazy val canonicalized: Expression = {
    // SPARK-32307: `ExpressionEncoder` can't be canonicalized, and technically we don't
    // need it to identify a `ScalaUDF`.
    copy(children = children.map(_.canonicalized), inputEncoders = Nil, outputEncoder = None)
  }

  /**
   * The analyzer should be aware of Scala primitive types so as to make the
   * UDF return null if there is any null input value of these types. On the
   * other hand, Java UDFs can only have boxed types, thus this will return
   * Nil(has same effect with all false) and analyzer will skip null-handling
   * on them.
   */
  lazy val inputPrimitives: Seq[Boolean] = {
    inputEncoders.map { encoderOpt =>
      // It's possible that some of the inputs don't have a specific encoder(e.g. `Any`)
      if (encoderOpt.isDefined) {
        val encoder = encoderOpt.get
        if (encoder.isSerializedAsStruct) {
          // struct type is not primitive
          false
        } else {
          // `nullable` is false iff the type is primitive
          !encoder.schema.head.nullable
        }
      } else {
        // Any type is not primitive
        false
      }
    }
  }

  /**
   * The expected input types of this UDF, used to perform type coercion. If we do
   * not want to perform coercion, simply use "Nil". Note that it would've been
   * better to use Option of Seq[DataType] so we can use "None" as the case for no
   * type coercion. However, that would require more refactoring of the codebase.
   */
  def inputTypes: Seq[AbstractDataType] = {
    inputEncoders.map { encoderOpt =>
      if (encoderOpt.isDefined) {
        val encoder = encoderOpt.get
        if (encoder.isSerializedAsStruct) {
          encoder.schema
        } else {
          encoder.schema.head.dataType
        }
      } else {
        AnyDataType
      }
    }
  }

  /**
   * Create the converter which converts the scala data type to the catalyst data type for
   * the return data type of udf function. We'd use `ExpressionEncoder` to create the
   * converter for typed ScalaUDF only, since its the only case where we know the type tag
   * of the return data type of udf function.
   */
  private def catalystConverter: Any => Any = outputEncoder.map { enc =>
    val toRow = enc.createSerializer().asInstanceOf[Any => Any]
    if (enc.isSerializedAsStructForTopLevel) {
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow]
    } else {
      value: Any =>
        if (value == null) null else toRow(value).asInstanceOf[InternalRow].get(0, dataType)
    }
  }.getOrElse(createToCatalystConverter(dataType))

  /**
   * Create the converter which converts the catalyst data type to the scala data type.
   * We use `CatalystTypeConverters` to create the converter for:
   *   - UDF which doesn't provide inputEncoders, e.g., untyped Scala UDF and Java UDF
   *   - type which isn't supported by `ExpressionEncoder`, e.g., Any
   *   - primitive types, in order to use `identity` for better performance
   * For other cases like case class, Option[T], we use `ExpressionEncoder` instead since
   * `CatalystTypeConverters` doesn't support these data types.
   *
   * @param i the index of the child
   * @param dataType the output data type of the i-th child
   * @return the converter and a boolean value to indicate whether the converter is
   *         created by using `ExpressionEncoder`.
   */
  private def scalaConverter(i: Int, dataType: DataType): (Any => Any, Boolean) = {
    val useEncoder =
      !(inputEncoders.isEmpty || // for untyped Scala UDF and Java UDF
      inputEncoders(i).isEmpty || // for types aren't supported by encoder, e.g. Any
      inputPrimitives(i)) // for primitive types

    if (useEncoder) {
      val enc = inputEncoders(i).get
      val fromRow = enc.createDeserializer()
      val converter = if (enc.isSerializedAsStructForTopLevel) {
        row: Any => fromRow(row.asInstanceOf[InternalRow])
      } else {
        val inputRow = new GenericInternalRow(1)
        value: Any => inputRow.update(0, value); fromRow(inputRow)
      }
      (converter, true)
    } else { // use CatalystTypeConverters
      (catalystCreateToScalaConverter(dataType), false)
    }
  }

  private def createToScalaConverter(i: Int, dataType: DataType): Any => Any =
    scalaConverter(i, dataType)._1

  // scalastyle:off line.size.limit

  /** This method has been generated by this script

    (1 to 22).map { x =>
      val anys = (1 to x).map(x => "Any").reduce(_ + ", " + _)
      val childs = (0 to x - 1).map(x => s"val child$x = children($x)").reduce(_ + "\n  " + _)
      val converters = (0 to x - 1).map(x => s"lazy val converter$x = createToScalaConverter($x, child$x.dataType)").reduce(_ + "\n  " + _)
      val evals = (0 to x - 1).map(x => s"converter$x(child$x.eval(input))").reduce(_ + ",\n      " + _)

      s"""case $x =>
      val func = function.asInstanceOf[($anys) => Any]
      $childs
      $converters
      (input: InternalRow) => {
        func(
          $evals)
      }
      """
    }.foreach(println)

  */
  private[this] val f = children.size match {
    case 0 =>
      val func = function.asInstanceOf[() => Any]
      (input: InternalRow) => {
        func()
      }

    case 1 =>
      val func = function.asInstanceOf[(Any) => Any]
      val child0 = children(0)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)))
      }

    case 2 =>
      val func = function.asInstanceOf[(Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)))
      }

    case 3 =>
      val func = function.asInstanceOf[(Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)))
      }

    case 4 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)))
      }

    case 5 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)))
      }

    case 6 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)))
      }

    case 7 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)))
      }

    case 8 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)))
      }

    case 9 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)))
      }

    case 10 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)))
      }

    case 11 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)))
      }

    case 12 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)))
      }

    case 13 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)))
      }

    case 14 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)))
      }

    case 15 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)))
      }

    case 16 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)))
      }

    case 17 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)))
      }

    case 18 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)))
      }

    case 19 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)))
      }

    case 20 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      val child19 = children(19)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      lazy val converter19 = createToScalaConverter(19, child19.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)),
          converter19(child19.eval(input)))
      }

    case 21 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      val child19 = children(19)
      val child20 = children(20)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      lazy val converter19 = createToScalaConverter(19, child19.dataType)
      lazy val converter20 = createToScalaConverter(20, child20.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)),
          converter19(child19.eval(input)),
          converter20(child20.eval(input)))
      }

    case 22 =>
      val func = function.asInstanceOf[(Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any) => Any]
      val child0 = children(0)
      val child1 = children(1)
      val child2 = children(2)
      val child3 = children(3)
      val child4 = children(4)
      val child5 = children(5)
      val child6 = children(6)
      val child7 = children(7)
      val child8 = children(8)
      val child9 = children(9)
      val child10 = children(10)
      val child11 = children(11)
      val child12 = children(12)
      val child13 = children(13)
      val child14 = children(14)
      val child15 = children(15)
      val child16 = children(16)
      val child17 = children(17)
      val child18 = children(18)
      val child19 = children(19)
      val child20 = children(20)
      val child21 = children(21)
      lazy val converter0 = createToScalaConverter(0, child0.dataType)
      lazy val converter1 = createToScalaConverter(1, child1.dataType)
      lazy val converter2 = createToScalaConverter(2, child2.dataType)
      lazy val converter3 = createToScalaConverter(3, child3.dataType)
      lazy val converter4 = createToScalaConverter(4, child4.dataType)
      lazy val converter5 = createToScalaConverter(5, child5.dataType)
      lazy val converter6 = createToScalaConverter(6, child6.dataType)
      lazy val converter7 = createToScalaConverter(7, child7.dataType)
      lazy val converter8 = createToScalaConverter(8, child8.dataType)
      lazy val converter9 = createToScalaConverter(9, child9.dataType)
      lazy val converter10 = createToScalaConverter(10, child10.dataType)
      lazy val converter11 = createToScalaConverter(11, child11.dataType)
      lazy val converter12 = createToScalaConverter(12, child12.dataType)
      lazy val converter13 = createToScalaConverter(13, child13.dataType)
      lazy val converter14 = createToScalaConverter(14, child14.dataType)
      lazy val converter15 = createToScalaConverter(15, child15.dataType)
      lazy val converter16 = createToScalaConverter(16, child16.dataType)
      lazy val converter17 = createToScalaConverter(17, child17.dataType)
      lazy val converter18 = createToScalaConverter(18, child18.dataType)
      lazy val converter19 = createToScalaConverter(19, child19.dataType)
      lazy val converter20 = createToScalaConverter(20, child20.dataType)
      lazy val converter21 = createToScalaConverter(21, child21.dataType)
      (input: InternalRow) => {
        func(
          converter0(child0.eval(input)),
          converter1(child1.eval(input)),
          converter2(child2.eval(input)),
          converter3(child3.eval(input)),
          converter4(child4.eval(input)),
          converter5(child5.eval(input)),
          converter6(child6.eval(input)),
          converter7(child7.eval(input)),
          converter8(child8.eval(input)),
          converter9(child9.eval(input)),
          converter10(child10.eval(input)),
          converter11(child11.eval(input)),
          converter12(child12.eval(input)),
          converter13(child13.eval(input)),
          converter14(child14.eval(input)),
          converter15(child15.eval(input)),
          converter16(child16.eval(input)),
          converter17(child17.eval(input)),
          converter18(child18.eval(input)),
          converter19(child19.eval(input)),
          converter20(child20.eval(input)),
          converter21(child21.eval(input)))
      }
  }

  // scalastyle:on line.size.limit
  override def doGenCode(
      ctx: CodegenContext,
      ev: ExprCode): ExprCode = {
    val converterClassName = classOf[Any => Any].getName

    // The type converters for inputs and the result
    val (converters, useEncoders): (Array[Any => Any], Array[Boolean]) =
      (children.zipWithIndex.map { case (c, i) =>
        scalaConverter(i, c.dataType)
      }.toArray :+ (catalystConverter, false)).unzip
    val convertersTerm = ctx.addReferenceObj("converters", converters, s"$converterClassName[]")
    val resultTerm = ctx.freshName("result")

    // codegen for children expressions
    val evals = children.map(_.genCode(ctx))

    // Generate the codes for expressions and calling user-defined function
    // We need to get the boxedType of dataType's javaType here. Because for the dataType
    // such as IntegerType, its javaType is `int` and the returned type of user-defined
    // function is Object. Trying to convert an Object to `int` will cause casting exception.
    val evalCode = evals.map(_.code).mkString("\n")
    val (funcArgs, initArgs) = evals.zipWithIndex.zip(children.map(_.dataType)).map {
      case ((eval, i), dt) =>
        val argTerm = ctx.freshName("arg")
        // Check `inputPrimitives` when it's not empty in order to figure out the Option
        // type as non primitive type, e.g., Option[Int]. Fall back to `isPrimitive` when
        // `inputPrimitives` is empty for other cases, e.g., Java UDF, untyped Scala UDF
        val primitive = (inputPrimitives.isEmpty && isPrimitive(dt)) ||
          (inputPrimitives.nonEmpty && inputPrimitives(i))
        val initArg = if (primitive) {
          val convertedTerm = ctx.freshName("conv")
          s"""
             |${CodeGenerator.boxedType(dt)} $convertedTerm = ${eval.value};
             |Object $argTerm = ${eval.isNull} ? null : $convertedTerm;
           """.stripMargin
        } else if (useEncoders(i)) {
          s"""
             |Object $argTerm = null;
             |if (${eval.isNull}) {
             |  $argTerm = $convertersTerm[$i].apply(null);
             |} else {
             |  $argTerm = $convertersTerm[$i].apply(${eval.value});
             |}
          """.stripMargin
        } else {
          s"Object $argTerm = ${eval.isNull} ? null : $convertersTerm[$i].apply(${eval.value});"
        }
        (argTerm, initArg)
    }.unzip

    val udf = ctx.addReferenceObj("udf", function, s"scala.Function${children.length}")
    val getFuncResult = s"$udf.apply(${funcArgs.mkString(", ")})"
    val resultConverter = s"$convertersTerm[${children.length}]"
    val boxedType = CodeGenerator.boxedType(dataType)

    val funcInvocation = if (isPrimitive(dataType)
        // If the output is nullable, the returned value must be unwrapped from the Option
        && !nullable) {
      s"$resultTerm = ($boxedType)$getFuncResult"
    } else {
      s"$resultTerm = ($boxedType)$resultConverter.apply($getFuncResult)"
    }
    val callFunc =
      s"""
         |$boxedType $resultTerm = null;
         |try {
         |  $funcInvocation;
         |} catch (Throwable e) {
         |  throw QueryExecutionErrors.failedExecuteUserDefinedFunctionError(
         |    "$funcCls", "$inputTypesString", "$outputType", e);
         |}
       """.stripMargin

    ev.copy(code =
      code"""
         |$evalCode
         |${initArgs.mkString("\n")}
         |$callFunc
         |
         |boolean ${ev.isNull} = $resultTerm == null;
         |${CodeGenerator.javaType(dataType)} ${ev.value} = ${CodeGenerator.defaultValue(dataType)};
         |if (!${ev.isNull}) {
         |  ${ev.value} = $resultTerm;
         |}
       """.stripMargin)
  }

  private[this] val resultConverter = catalystConverter

  lazy val funcCls = Utils.getSimpleName(function.getClass)
  lazy val inputTypesString = children.map(_.dataType.catalogString).mkString(", ")
  lazy val outputType = dataType.catalogString

  override def eval(input: InternalRow): Any = {
    val result = try {
      f(input)
    } catch {
      case e: Exception =>
        throw QueryExecutionErrors.failedExecuteUserDefinedFunctionError(
          funcCls, inputTypesString, outputType, e)
    }

    resultConverter(result)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): ScalaUDF =
    copy(children = newChildren)
}
