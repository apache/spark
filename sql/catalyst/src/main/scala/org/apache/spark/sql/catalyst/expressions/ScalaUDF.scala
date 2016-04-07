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

import java.lang.reflect.InvocationTargetException

import scala.reflect.runtime.universe._

import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * User-defined function.
 * @param function  The user defined scala function to run.
 *                  Note that if you use primitive parameters, you are not able to check if it is
 *                  null or not, and the UDF will return null for you if the primitive input is
 *                  null. Use boxed type or [[Option]] if you wanna do the null-handling yourself.
 * @param dataType  Return type of function.
 * @param children  The input expressions of this UDF.
 * @param inputTypes  The expected input types of this UDF, used to perform type coercion. If we do
 *                    not want to perform coercion, simply use "Nil". Note that it would've been
 *                    better to use Option of Seq[DataType] so we can use "None" as the case for no
 *                    type coercion. However, that would require more refactoring of the codebase.
 */
case class ScalaUDF(
    function: AnyRef,
    dataType: DataType,
    children: Seq[Expression],
    inputTypes: Seq[DataType] = Nil)
  extends Expression with ImplicitCastInputTypes with NonSQLExpression {

  override def nullable: Boolean = true

  override def toString: String = s"UDF(${children.mkString(", ")})"

  // The dataType used in output expression encoder
  // The return values of UDF will be encoded in a field in an internal row
  def getDataType(): StructType = StructType(StructField("_c0", dataType) :: Nil)

  lazy val inputSchema: StructType = {
    val fields = if (inputTypes == Nil) {
      // from the deprecated callUDF codepath
      children.zipWithIndex.map { case (e, i) =>
        StructField(s"_c$i", e.dataType)
      }
    } else {
      inputTypes.zipWithIndex.map { case (t, i) =>
        StructField(s"_c$i", t)
      }
    }
    StructType(fields)
  }

  override def genCode(
      ctx: CodegenContext,
      ev: ExprCode): String = {

    val scalaUDFClassName = classOf[ScalaUDF].getName
    val scalaUDFObject = ctx.addReferenceObj("scalaUDF", this, scalaUDFClassName)

    val converterClassName = classOf[Any => Any].getName
    val typeConvertersClassName = CatalystTypeConverters.getClass.getName + ".MODULE$"
    val expressionClassName = classOf[Expression].getName
    val expressionEncoderClassName = classOf[ExpressionEncoder[Row]].getName
    val rowEncoderClassName = RowEncoder.getClass.getName + ".MODULE$"
    val structTypeClassName = StructType.getClass.getName + ".MODULE$"
    val rowClassName = Row.getClass.getName + ".MODULE$"
    val rowClass = classOf[Row].getName
    val internalRowClassName = classOf[InternalRow].getName
    // scalastyle:off
    // JavaConversions has been banned for implicit conversion between Java and Scala types.
    // However, we are not going to use it in Scala side but use it in generated Java codes.
    // JavaConverters doesn't provide simple and direct method to call for the purpose here.
    // So we turn off scalastyle here temporarily.
    val javaConversionClassName = scala.collection.JavaConversions.getClass.getName + ".MODULE$"
    // scalastyle:on

    // Generate code for input encoder
    val inputExpressionEncoderTerm = ctx.freshName("inputExpressionEncoder")
    ctx.addMutableState(expressionEncoderClassName, inputExpressionEncoderTerm,
      s"this.$inputExpressionEncoderTerm = ($expressionEncoderClassName)$rowEncoderClassName" +
        s".apply($scalaUDFObject.inputSchema());")

    // Generate code for output encoder
    val outputExpressionEncoderTerm = ctx.freshName("outputExpressionEncoder")
    ctx.addMutableState(expressionEncoderClassName, outputExpressionEncoderTerm,
      s"this.$outputExpressionEncoderTerm = ($expressionEncoderClassName)$rowEncoderClassName" +
        s".apply($scalaUDFObject.getDataType());")

    val resultTerm = ctx.freshName("result")

    // Initialize user-defined function
    val funcClassName = s"scala.Function${children.size}"

    val funcTerm = ctx.freshName("udf")
    ctx.addMutableState(funcClassName, funcTerm,
      s"this.$funcTerm = ($funcClassName)($scalaUDFObject.function());")

    // codegen for children expressions
    val evals = children.map(_.gen(ctx))
    val (converters, evalsArgs) = evals.zipWithIndex.map { case (eval, i) =>
      val argTerm = ctx.freshName("arg")
      val convert = s"${ctx.boxedType(children(i).dataType)} $argTerm = ${eval.isNull} ? null " +
        s": new ${ctx.boxedType(children(i).dataType)}(${eval.value});"
      (convert, argTerm)
    }.unzip
    val evalsAsSeq = s"$javaConversionClassName.collectionAsScalaIterable" +
      s"(java.util.Arrays.asList(${evalsArgs.mkString(", ")})).toList()"

    // Encode children expression results to Scala objects
    val inputInternalRowTerm = ctx.freshName("inputRow")
    val inputInternalRow = s"$rowClass $inputInternalRowTerm = " +
      s"($rowClass)$inputExpressionEncoderTerm.fromRow(InternalRow.fromSeq($evalsAsSeq));"

    // Generate the codes for expressions and calling user-defined function
    // We need to get the boxedType of dataType's javaType here. Because for the dataType
    // such as IntegerType, its javaType is `int` and the returned type of user-defined
    // function is Object. Trying to convert an Object to `int` will cause casting exception.
    val evalCode = evals.map(_.code).mkString

    val funcArguments = (0 until children.size).map { i =>
      s"$inputInternalRowTerm.get($i)"
    }.mkString(", ")

    val rowParametersTerm = ctx.freshName("rowParameters")
    val innerRow = s"$rowClass $rowParametersTerm = $rowClassName.apply(" +
      s"$javaConversionClassName.collectionAsScalaIterable" +
      s"(java.util.Arrays.asList($funcTerm.apply($funcArguments))).toList());"

    // Encode Scala objects of UDF return values to Spark SQL internal row
    val internalRowTerm = ctx.freshName("internalRow")
    val internalRow = s"$internalRowClassName $internalRowTerm = ($internalRowClassName)" +
      s"${outputExpressionEncoderTerm}.toRow($rowParametersTerm).copy();"

    // UDF return values are encoded as the field 0 as StructType in the internal row
    // We extract it back
    val udfDataType = s"$scalaUDFObject.dataType()"
    val callFunc = s"${ctx.boxedType(dataType)} $resultTerm = " +
      s"(${ctx.boxedType(dataType)}) $internalRowTerm.get(0, $udfDataType);"

    evalCode + s"""
      ${converters.mkString("\n")}
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};

      $inputInternalRow
      $innerRow
      $internalRow
      $callFunc

      boolean ${ev.isNull} = $resultTerm == null;
      if (!${ev.isNull}) {
        ${ev.value} = $resultTerm;
      }
    """
  }

  lazy val inputEncoder: ExpressionEncoder[Row] = RowEncoder(inputSchema)
  lazy val outputEncoder: ExpressionEncoder[Row] =
      RowEncoder(StructType(StructField("_c0", dataType) :: Nil))

  lazy val reflectedFunc =
    runtimeMirror(function.getClass.getClassLoader).reflect(function)
  lazy val applyMethods = reflectedFunc.symbol.typeSignature.member(newTermName("apply"))
    .asTerm.alternatives
  lazy val invokeMethod = reflectedFunc.reflectMethod(applyMethods(0).asMethod)

  override def eval(input: InternalRow): Any = {
    val projected = InternalRow.fromSeq(children.map(_.eval(input)))
    val cRow = inputEncoder.fromRow(projected)

    try {
      val callRet = invokeMethod.apply(cRow.toSeq: _*)
      outputEncoder.toRow(Row(callRet)).copy().asInstanceOf[InternalRow].get(0, dataType)
    } catch {
      // When exception is thrown in UDF, an InvocationTargetException will be thrown.
      // We get and re-throw the cause exception.
      case e: InvocationTargetException => throw e.getTargetException
    }
  }
}
