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
import org.apache.spark.annotation.Private
import org.apache.spark.sql.{catalyst, Row}
import org.apache.spark.sql.catalyst.expressions._

/**
 * Inherits some default implementation for Java from `Ordering[Row]`
 */
@Private
class BaseOrdering extends Ordering[catalyst.InternalRow] {
  def compare(a: catalyst.InternalRow, b: catalyst.InternalRow): Int = {
    throw new UnsupportedOperationException
  }
}

/**
 * Generates bytecode for an [[Ordering]] of [[Row Rows]] for a given set of
 * [[Expression Expressions]].
 */
object GenerateOrdering
    extends CodeGenerator[Seq[SortOrder], Ordering[catalyst.InternalRow]] with Logging {
  import scala.reflect.runtime.universe._

  protected def canonicalize(in: Seq[SortOrder]): Seq[SortOrder] =
    in.map(ExpressionCanonicalizer.execute(_).asInstanceOf[SortOrder])

  protected def bind(in: Seq[SortOrder], inputSchema: Seq[Attribute]): Seq[SortOrder] =
    in.map(BindReferences.bindReference(_, inputSchema))

  protected def create(ordering: Seq[SortOrder]): Ordering[catalyst.InternalRow] = {
    val a = newTermName("a")
    val b = newTermName("b")
    val ctx = newCodeGenContext()

    val comparisons = ordering.zipWithIndex.map { case (order, i) =>
      val evalA = order.child.gen(ctx)
      val evalB = order.child.gen(ctx)
      val asc = order.direction == Ascending
      s"""
          i = $a;
          ${evalA.code}
          i = $b;
          ${evalB.code}
          if (${evalA.isNull} && ${evalB.isNull}) {
            // Nothing
          } else if (${evalA.isNull}) {
            return ${if (order.direction == Ascending) "-1" else "1"};
          } else if (${evalB.isNull}) {
            return ${if (order.direction == Ascending) "1" else "-1"};
          } else {
            int comp = ${ctx.genComp(order.child.dataType, evalA.primitive, evalB.primitive)};
            if (comp != 0) {
              return ${if (asc) "comp" else "-comp"};
            }
          }
      """
    }.mkString("\n")

    val code = s"""
      import org.apache.spark.sql.catalyst.InternalRow;

      public SpecificOrdering generate($exprType[] expr) {
        return new SpecificOrdering(expr);
      }

      class SpecificOrdering extends ${typeOf[BaseOrdering]} {

        private $exprType[] expressions = null;

        public SpecificOrdering($exprType[] expr) {
          expressions = expr;
        }

        @Override
        public int compare(InternalRow a, InternalRow b) {
          InternalRow i = null;  // Holds current row being evaluated.
          $comparisons
          return 0;
        }
      }"""

    logDebug(s"Generated Ordering: $code")

    val c = compile(code)
    // fetch the only one method `generate(Expression[])`
    val m = c.getDeclaredMethods()(0)
    m.invoke(c.newInstance(), ctx.references.toArray).asInstanceOf[BaseOrdering]
  }
}
