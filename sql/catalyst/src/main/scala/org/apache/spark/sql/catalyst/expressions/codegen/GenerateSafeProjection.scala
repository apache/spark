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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayBasedMapData}
import org.apache.spark.sql.types._


/**
 * Generates byte code that produces a [[MutableRow]] object (not an [[UnsafeRow]]) that can update
 * itself based on a new input [[InternalRow]] for a fixed set of [[Expression Expressions]].
 */
object GenerateSafeProjection extends CodeGenerator[Seq[Expression], Projection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  private def createCodeForStruct(
      ctx: CodeGenContext,
      input: String,
      schema: StructType): GeneratedExpressionCode = {
    val tmp = ctx.freshName("tmp")
    val output = ctx.freshName("safeRow")
    val values = ctx.freshName("values")
    // These expressions could be splitted into multiple functions
    ctx.addMutableState("Object[]", values, s"this.$values = null;")

    val rowClass = classOf[GenericInternalRow].getName

    val fieldWriters = schema.map(_.dataType).zipWithIndex.map { case (dt, i) =>
      val converter = convertToSafe(ctx, ctx.getValue(tmp, dt, i.toString), dt)
      s"""
        if (!$tmp.isNullAt($i)) {
          ${converter.code}
          $values[$i] = ${converter.value};
        }
      """
    }
    val allFields = ctx.splitExpressions(tmp, fieldWriters)
    val code = s"""
      final InternalRow $tmp = $input;
      this.$values = new Object[${schema.length}];
      $allFields
      final InternalRow $output = new $rowClass($values);
    """

    GeneratedExpressionCode(code, "false", output)
  }

  private def createCodeForArray(
      ctx: CodeGenContext,
      input: String,
      elementType: DataType): GeneratedExpressionCode = {
    val tmp = ctx.freshName("tmp")
    val output = ctx.freshName("safeArray")
    val values = ctx.freshName("values")
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")
    val arrayClass = classOf[GenericArrayData].getName

    val elementConverter = convertToSafe(ctx, ctx.getValue(tmp, elementType, index), elementType)
    val code = s"""
      final ArrayData $tmp = $input;
      final int $numElements = $tmp.numElements();
      final Object[] $values = new Object[$numElements];
      for (int $index = 0; $index < $numElements; $index++) {
        if (!$tmp.isNullAt($index)) {
          ${elementConverter.code}
          $values[$index] = ${elementConverter.value};
        }
      }
      final ArrayData $output = new $arrayClass($values);
    """

    GeneratedExpressionCode(code, "false", output)
  }

  private def createCodeForMap(
      ctx: CodeGenContext,
      input: String,
      keyType: DataType,
      valueType: DataType): GeneratedExpressionCode = {
    val tmp = ctx.freshName("tmp")
    val output = ctx.freshName("safeMap")
    val mapClass = classOf[ArrayBasedMapData].getName

    val keyConverter = createCodeForArray(ctx, s"$tmp.keyArray()", keyType)
    val valueConverter = createCodeForArray(ctx, s"$tmp.valueArray()", valueType)
    val code = s"""
      final MapData $tmp = $input;
      ${keyConverter.code}
      ${valueConverter.code}
      final MapData $output = new $mapClass(${keyConverter.value}, ${valueConverter.value});
    """

    GeneratedExpressionCode(code, "false", output)
  }

  private def convertToSafe(
      ctx: CodeGenContext,
      input: String,
      dataType: DataType): GeneratedExpressionCode = dataType match {
    case s: StructType => createCodeForStruct(ctx, input, s)
    case ArrayType(elementType, _) => createCodeForArray(ctx, input, elementType)
    case MapType(keyType, valueType, _) => createCodeForMap(ctx, input, keyType, valueType)
    // UTF8String act as a pointer if it's inside UnsafeRow, so copy it to make it safe.
    case StringType => GeneratedExpressionCode("", "false", s"$input.clone()")
    case udt: UserDefinedType[_] => convertToSafe(ctx, input, udt.sqlType)
    case _ => GeneratedExpressionCode("", "false", input)
  }

  protected def create(expressions: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val expressionCodes = expressions.zipWithIndex.map {
      case (NoOp, _) => ""
      case (e, i) =>
        val evaluationCode = e.gen(ctx)
        val converter = convertToSafe(ctx, evaluationCode.value, e.dataType)
        evaluationCode.code +
          s"""
            if (${evaluationCode.isNull}) {
              mutableRow.setNullAt($i);
            } else {
              ${converter.code}
              ${ctx.setColumn("mutableRow", e.dataType, i, converter.value)};
            }
          """
    }
    val allExpressions = ctx.splitExpressions(ctx.INPUT_ROW, expressionCodes)
    val codeBody = s"""
      public java.lang.Object generate($exprType[] expr) {
        return new SpecificSafeProjection(expr);
      }

      class SpecificSafeProjection extends ${classOf[BaseProjection].getName} {

        private $exprType[] expressions;
        private $mutableRowType mutableRow;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificSafeProjection($exprType[] expr) {
          expressions = expr;
          mutableRow = new $genericMutableRowType(${expressions.size});
          ${initMutableStates(ctx)}
        }

        public java.lang.Object apply(java.lang.Object _i) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) _i;
          $allExpressions
          return mutableRow;
        }
      }
    """

    val code = new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[Projection]
  }
}
