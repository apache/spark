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

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenContext, GeneratedExpressionCode}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.DataType

case class Coalesce(children: Seq[Expression]) extends Expression {

  /** Coalesce is nullable if all of its children are nullable, or if it has no children. */
  override def nullable: Boolean = children.forall(_.nullable)

  // Coalesce is foldable if all children are foldable.
  override def foldable: Boolean = children.forall(_.foldable)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children == Nil) {
      TypeCheckResult.TypeCheckFailure("input to function coalesce cannot be empty")
    } else {
      TypeUtils.checkForSameTypeInputExpr(children.map(_.dataType), "function coalesce")
    }
  }

  override def dataType: DataType = children.head.dataType

  override def eval(input: InternalRow): Any = {
    var result: Any = null
    val childIterator = children.iterator
    while (childIterator.hasNext && result == null) {
      result = childIterator.next().eval(input)
    }
    result
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    s"""
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.primitive} = ${ctx.defaultValue(dataType)};
    """ +
    children.map { e =>
      val eval = e.gen(ctx)
      s"""
        if (${ev.isNull}) {
          ${eval.code}
          if (!${eval.isNull}) {
            ${ev.isNull} = false;
            ${ev.primitive} = ${eval.primitive};
          }
        }
      """
    }.mkString("\n")
  }
}

case class IsNull(child: Expression) extends UnaryExpression with Predicate {
  override def nullable: Boolean = false

  override def eval(input: InternalRow): Any = {
    child.eval(input) == null
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    ev.isNull = "false"
    ev.primitive = eval.isNull
    eval.code
  }

  override def toString: String = s"IS NULL $child"
}

case class IsNotNull(child: Expression) extends UnaryExpression with Predicate {
  override def nullable: Boolean = false
  override def toString: String = s"IS NOT NULL $child"

  override def eval(input: InternalRow): Any = {
    child.eval(input) != null
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val eval = child.gen(ctx)
    ev.isNull = "false"
    ev.primitive = s"(!(${eval.isNull}))"
    eval.code
  }
}

/**
 * A predicate that is evaluated to be true if there are at least `n` non-null values.
 */
case class AtLeastNNonNulls(n: Int, children: Seq[Expression]) extends Predicate {
  override def nullable: Boolean = false
  override def foldable: Boolean = false
  override def toString: String = s"AtLeastNNulls(n, ${children.mkString(",")})"

  private[this] val childrenArray = children.toArray

  override def eval(input: InternalRow): Boolean = {
    var numNonNulls = 0
    var i = 0
    while (i < childrenArray.length && numNonNulls < n) {
      if (childrenArray(i).eval(input) != null) {
        numNonNulls += 1
      }
      i += 1
    }
    numNonNulls >= n
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val nonnull = ctx.freshName("nonnull")
    val code = children.map { e =>
      val eval = e.gen(ctx)
      s"""
        if ($nonnull < $n) {
          ${eval.code}
          if (!${eval.isNull}) {
            $nonnull += 1;
          }
        }
      """
    }.mkString("\n")
    s"""
      int $nonnull = 0;
      $code
      boolean ${ev.isNull} = false;
      boolean ${ev.primitive} = $nonnull >= $n;
     """
  }
}
