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

package org.apache.spark.sql.catalyst.expressions.codegen

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReferences
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._

/**
 * Java cannot access Projection (in package object)
 */
abstract class BaseProjection extends Projection {}

/**
 * Generates byte code that produces a [[InternalRow]] object (not an [[UnsafeRow]]) that can update
 * itself based on a new input [[InternalRow]] for a fixed set of [[Expression Expressions]].
 */
object GenerateSafeProjection extends CodeGenerator[Seq[Expression], Projection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    bindReferences(in, inputSchema)

  private def createCodeForStruct(
      ctx: CodegenContext,
      input: String,
      schema: StructType): ExprCode = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val output = ctx.freshName("safeRow")
    val values = ctx.freshName("values")

    val rowClass = classOf[GenericInternalRow].getName

    val fieldWriters = schema.map(_.dataType).zipWithIndex.map { case (dt, i) =>
      val converter = convertToSafe(
        ctx,
        JavaCode.expression(CodeGenerator.getValue(tmpInput, dt, i.toString), dt),
        dt)
      s"""
        if (!$tmpInput.isNullAt($i)) {
          ${converter.code}
          $values[$i] = ${converter.value};
        }
      """
    }
    val allFields = ctx.splitExpressions(
      expressions = fieldWriters,
      funcName = "writeFields",
      arguments = Seq("InternalRow" -> tmpInput, "Object[]" -> values)
    )
    val code =
      code"""
         |final InternalRow $tmpInput = $input;
         |final Object[] $values = new Object[${schema.length}];
         |$allFields
         |final InternalRow $output = new $rowClass($values);
       """.stripMargin

    ExprCode(code, FalseLiteral, JavaCode.variable(output, classOf[InternalRow]))
  }

  private def createCodeForArray(
      ctx: CodegenContext,
      input: String,
      elementType: DataType): ExprCode = {
    // Puts `input` in a local variable to avoid to re-evaluate it if it's a statement.
    val tmpInput = ctx.freshName("tmpInput")
    val output = ctx.freshName("safeArray")
    val values = ctx.freshName("values")
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")
    val arrayClass = classOf[GenericArrayData].getName

    val elementConverter = convertToSafe(
      ctx,
      JavaCode.expression(CodeGenerator.getValue(tmpInput, elementType, index), elementType),
      elementType)
    val code = code"""
      final ArrayData $tmpInput = $input;
      final int $numElements = $tmpInput.numElements();
      final Object[] $values = new Object[$numElements];
      for (int $index = 0; $index < $numElements; $index++) {
        if (!$tmpInput.isNullAt($index)) {
          ${elementConverter.code}
          $values[$index] = ${elementConverter.value};
        }
      }
      final ArrayData $output = new $arrayClass($values);
    """

    ExprCode(code, FalseLiteral, JavaCode.variable(output, classOf[ArrayData]))
  }

  private def createCodeForMap(
      ctx: CodegenContext,
      input: String,
      keyType: DataType,
      valueType: DataType): ExprCode = {
    val tmpInput = ctx.freshName("tmpInput")
    val output = ctx.freshName("safeMap")
    val mapClass = classOf[ArrayBasedMapData].getName

    val keyConverter = createCodeForArray(ctx, s"$tmpInput.keyArray()", keyType)
    val valueConverter = createCodeForArray(ctx, s"$tmpInput.valueArray()", valueType)
    val code = code"""
      final MapData $tmpInput = $input;
      ${keyConverter.code}
      ${valueConverter.code}
      final MapData $output = new $mapClass(${keyConverter.value}, ${valueConverter.value});
    """

    ExprCode(code, FalseLiteral, JavaCode.variable(output, classOf[MapData]))
  }

  @tailrec
  private def convertToSafe(
      ctx: CodegenContext,
      input: ExprValue,
      dataType: DataType): ExprCode = dataType match {
    case s: StructType => createCodeForStruct(ctx, input, s)
    case ArrayType(elementType, _) => createCodeForArray(ctx, input, elementType)
    case MapType(keyType, valueType, _) => createCodeForMap(ctx, input, keyType, valueType)
    case udt: UserDefinedType[_] => convertToSafe(ctx, input, udt.sqlType)
    case _ => ExprCode(FalseLiteral, input)
  }

  protected def create(expressions: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val expressionCodes = expressions.zipWithIndex.map {
      case (NoOp, _) => ""
      case (e, i) =>
        val evaluationCode = e.genCode(ctx)
        val converter = convertToSafe(ctx, evaluationCode.value, e.dataType)
        evaluationCode.code.toString +
          s"""
            if (${evaluationCode.isNull}) {
              mutableRow.setNullAt($i);
            } else {
              ${converter.code}
              ${CodeGenerator.setColumn("mutableRow", e.dataType, i, converter.value)};
            }
          """
    }
    val allExpressions = ctx.splitExpressionsWithCurrentInputs(expressionCodes)

    val codeBody = s"""
      public java.lang.Object generate(Object[] references) {
        return new SpecificSafeProjection(references);
      }

      class SpecificSafeProjection extends ${classOf[BaseProjection].getName} {

        private Object[] references;
        private InternalRow mutableRow;
        ${ctx.declareMutableStates()}

        public SpecificSafeProjection(Object[] references) {
          this.references = references;
          mutableRow = (InternalRow) references[references.length - 1];
          ${ctx.initMutableStates()}
        }

        public void initialize(int partitionIndex) {
          ${ctx.initPartition()}
        }

        public java.lang.Object apply(java.lang.Object _i) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) _i;
          $allExpressions
          return mutableRow;
        }

        ${ctx.declareAddedFunctions()}
      }
    """

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val (clazz, _) = CodeGenerator.compile(code)
    val resultRow = new SpecificInternalRow(expressions.map(_.dataType))
    clazz.generate(ctx.references.toArray :+ resultRow).asInstanceOf[Projection]
  }
}
