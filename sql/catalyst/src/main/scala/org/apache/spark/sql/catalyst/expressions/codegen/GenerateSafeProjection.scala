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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.NoOp
import org.apache.spark.sql.types.{StringType, StructType, DataType}


/**
 * Generates byte code that produces a [[MutableRow]] object that can update itself based on a new
 * input [[InternalRow]] for a fixed set of [[Expression Expressions]].
 */
object GenerateSafeProjection extends CodeGenerator[Seq[Expression], Projection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  private def genUpdater(
      ctx: CodeGenContext,
      setter: String,
      dataType: DataType,
      ordinal: Int,
      value: String): String = {
    dataType match {
      case struct: StructType =>
        val rowTerm = ctx.freshName("row")
        val updates = struct.map(_.dataType).zipWithIndex.map { case (dt, i) =>
          val colTerm = ctx.freshName("col")
          s"""
            if ($value.isNullAt($i)) {
              $rowTerm.setNullAt($i);
            } else {
              ${ctx.javaType(dt)} $colTerm = ${ctx.getValue(value, dt, s"$i")};
              ${genUpdater(ctx, rowTerm, dt, i, colTerm)};
            }
           """
        }
        val allUpdates = ctx.splitExpressions(value, updates)
        s"""
          $genericMutableRowType $rowTerm = new $genericMutableRowType(${struct.fields.length});
          $allUpdates
          $setter.update($ordinal, $rowTerm.copy());
        """
      case _ =>
        ctx.setColumn(setter, dataType, ordinal, value)
    }
  }

  protected def create(expressions: Seq[Expression]): Projection = {
    val ctx = newCodeGenContext()
    val expressionCodes = expressions.zipWithIndex.map {
      case (NoOp, _) => ""
      case (e, i) =>
        val evaluationCode = e.gen(ctx)
        evaluationCode.code +
          s"""
            if (${evaluationCode.isNull}) {
              mutableRow.setNullAt($i);
            } else {
              ${genUpdater(ctx, "mutableRow", e.dataType, i, evaluationCode.primitive)};
            }
          """
    }
    val allExpressions = ctx.splitExpressions("i", expressionCodes)
    val code = s"""
      public Object generate($exprType[] expr) {
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

        public Object apply(Object _i) {
          InternalRow i = (InternalRow) _i;
          $allExpressions
          return mutableRow;
        }
      }
    """

    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    c.generate(ctx.references.toArray).asInstanceOf[Projection]
  }
}
