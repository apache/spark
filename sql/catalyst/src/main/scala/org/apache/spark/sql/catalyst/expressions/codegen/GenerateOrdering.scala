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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StructType

/**
 * Inherits some default implementation for Java from `Ordering[Row]`
 */
class BaseOrdering extends Ordering[InternalRow] {
  def compare(a: InternalRow, b: InternalRow): Int = {
    throw new UnsupportedOperationException
  }
}

/**
 * Generates bytecode for an [[Ordering]] of rows for a given set of expressions.
 */
object GenerateOrdering extends CodeGenerator[Seq[SortOrder], Ordering[InternalRow]] with Logging {

  protected def canonicalize(in: Seq[SortOrder]): Seq[SortOrder] =
    in.map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder])

  protected def bind(in: Seq[SortOrder], inputSchema: Seq[Attribute]): Seq[SortOrder] =
    in.map(BindReferences.bindReference(_, inputSchema))

  /**
   * Creates a code gen ordering for sorting this schema, in ascending order.
   */
  def create(schema: StructType): BaseOrdering = {
    create(schema.zipWithIndex.map { case (field, ordinal) =>
      SortOrder(BoundReference(ordinal, field.dataType, nullable = true), Ascending)
    })
  }

  /**
   * Generates the code for comparing a struct type according to its natural ordering
   * (i.e. ascending order by field 1, then field 2, ..., then field n.
   */
  def genComparisons(ctx: CodeGenContext, schema: StructType): String = {
    val ordering = schema.fields.map(_.dataType).zipWithIndex.map {
      case(dt, index) => new SortOrder(BoundReference(index, dt, nullable = true), Ascending)
    }
    genComparisons(ctx, ordering)
  }

  /**
   * Generates the code for ordering based on the given order.
   */
  def genComparisons(ctx: CodeGenContext, ordering: Seq[SortOrder]): String = {
    val comparisons = ordering.map { order =>
      val eval = order.child.gen(ctx)
      val asc = order.direction == Ascending
      val isNullA = ctx.freshName("isNullA")
      val primitiveA = ctx.freshName("primitiveA")
      val isNullB = ctx.freshName("isNullB")
      val primitiveB = ctx.freshName("primitiveB")
      s"""
          ${ctx.INPUT_ROW} = a;
          boolean $isNullA;
          ${ctx.javaType(order.child.dataType)} $primitiveA;
          {
            ${eval.code}
            $isNullA = ${eval.isNull};
            $primitiveA = ${eval.value};
          }
          ${ctx.INPUT_ROW} = b;
          boolean $isNullB;
          ${ctx.javaType(order.child.dataType)} $primitiveB;
          {
            ${eval.code}
            $isNullB = ${eval.isNull};
            $primitiveB = ${eval.value};
          }
          if ($isNullA && $isNullB) {
            // Nothing
          } else if ($isNullA) {
            return ${if (order.direction == Ascending) "-1" else "1"};
          } else if ($isNullB) {
            return ${if (order.direction == Ascending) "1" else "-1"};
          } else {
            int comp = ${ctx.genComp(order.child.dataType, primitiveA, primitiveB)};
            if (comp != 0) {
              return ${if (asc) "comp" else "-comp"};
            }
          }
      """
    }.mkString("\n")
    comparisons
  }

  protected def create(ordering: Seq[SortOrder]): BaseOrdering = {
    val ctx = newCodeGenContext()
    val comparisons = genComparisons(ctx, ordering)
    val code = s"""
      public SpecificOrdering generate($exprType[] expr) {
        return new SpecificOrdering(expr);
      }

      class SpecificOrdering extends ${classOf[BaseOrdering].getName} {

        private $exprType[] expressions;
        ${declareMutableStates(ctx)}
        ${declareAddedFunctions(ctx)}

        public SpecificOrdering($exprType[] expr) {
          expressions = expr;
          ${initMutableStates(ctx)}
        }

        public int compare(InternalRow a, InternalRow b) {
          InternalRow ${ctx.INPUT_ROW} = null;  // Holds current row being evaluated.
          $comparisons
          return 0;
        }
      }"""

    logDebug(s"Generated Ordering: ${CodeFormatter.format(code)}")

    compile(code).generate(ctx.references.toArray).asInstanceOf[BaseOrdering]
  }
}
