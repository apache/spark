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
import org.apache.spark.sql.types.DecimalType

// MutableProjection is not accessible in Java
abstract class BaseMutableProjection extends MutableProjection

/**
 * Generates byte code that produces a [[MutableRow]] object that can update itself based on a new
 * input [[InternalRow]] for a fixed set of [[Expression Expressions]].
 * It exposes a `target` method, which is used to set the row that will be updated.
 * The internal [[MutableRow]] object created internally is used only when `target` is not used.
 */
object GenerateMutableProjection extends CodeGenerator[Seq[Expression], () => MutableProjection] {

  protected def canonicalize(in: Seq[Expression]): Seq[Expression] =
    in.map(ExpressionCanonicalizer.execute)

  protected def bind(in: Seq[Expression], inputSchema: Seq[Attribute]): Seq[Expression] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(expressions: Seq[Expression]): (() => MutableProjection) = {
    val ctx = newCodeGenContext()
    val projectionCodes = expressions.zipWithIndex.map {
      case (NoOp, _) => ""
      case (e, i) =>
        val evaluationCode = e.gen(ctx)
        val isNull = s"isNull_$i"
        val value = s"value_$i"
        ctx.addMutableState("boolean", isNull, s"this.$isNull = true;")
        ctx.addMutableState(ctx.javaType(e.dataType), value,
          s"this.$value = ${ctx.defaultValue(e.dataType)};")
        s"""
          ${evaluationCode.code}
          this.$isNull = ${evaluationCode.isNull};
          this.$value = ${evaluationCode.value};
         """
    }
    val updates = expressions.zipWithIndex.map {
      case (NoOp, _) => ""
      case (e, i) =>
        if (e.dataType.isInstanceOf[DecimalType]) {
          // Can't call setNullAt on DecimalType, because we need to keep the offset
          s"""
            if (this.isNull_$i) {
              ${ctx.setColumn("mutableRow", e.dataType, i, null)};
            } else {
              ${ctx.setColumn("mutableRow", e.dataType, i, s"this.value_$i")};
            }
          """
        } else {
          s"""
            if (this.isNull_$i) {
              mutableRow.setNullAt($i);
            } else {
              ${ctx.setColumn("mutableRow", e.dataType, i, s"this.value_$i")};
            }
          """
        }
    }

    val allProjections = ctx.splitExpressions(ctx.INPUT_ROW, projectionCodes)
    val allUpdates = ctx.splitExpressions(ctx.INPUT_ROW, updates)

    val codeBody = s"""
      public java.lang.Object generate($exprType[] expr) {
        return new SpecificMutableProjection(expr);
      }

      class SpecificMutableProjection extends ${classOf[BaseMutableProjection].getName} {

        private $exprType[] expressions;
        private $mutableRowType mutableRow;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificMutableProjection($exprType[] expr) {
          expressions = expr;
          mutableRow = new $genericMutableRowType(${expressions.size});
          ${initMutableStates(ctx)}
        }

        public ${classOf[BaseMutableProjection].getName} target($mutableRowType row) {
          mutableRow = row;
          return this;
        }

        /* Provide immutable access to the last projected row. */
        public InternalRow currentValue() {
          return (InternalRow) mutableRow;
        }

        public java.lang.Object apply(java.lang.Object _i) {
          InternalRow ${ctx.INPUT_ROW} = (InternalRow) _i;
          $allProjections
          // copy all the results into MutableRow
          $allUpdates
          return mutableRow;
        }
      }
    """

    val code = new CodeAndComment(codeBody, ctx.getPlaceHolderToComments())
    logDebug(s"code for ${expressions.mkString(",")}:\n${CodeFormatter.format(code)}")

    val c = compile(code)
    () => {
      c.generate(ctx.references.toArray).asInstanceOf[MutableProjection]
    }
  }
}
