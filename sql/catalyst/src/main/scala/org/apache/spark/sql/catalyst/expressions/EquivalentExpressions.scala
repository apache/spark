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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable

/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class EquivalentExpressions {
  /**
   * Wrapper around an Expression that provides semantic equality.
   */
  case class Expr(e: Expression) {
    override def equals(o: Any): Boolean = o match {
      case other: Expr => e.semanticEquals(other.e)
      case _ => false
    }

    override def hashCode: Int = e.semanticHash()
  }

  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[Expr, mutable.MutableList[Expression]]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    if (expr.deterministic) {
      val e: Expr = Expr(expr)
      val f = equivalenceMap.get(e)
      if (f.isDefined) {
        f.get += expr
        true
      } else {
        equivalenceMap.put(e, mutable.MutableList(expr))
        false
      }
    } else {
      false
    }
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   * If ignoreLeaf is true, leaf nodes are ignored.
   */
  def addExprTree(
      root: Expression,
      ignoreLeaf: Boolean = true,
      skipReferenceToExpressions: Boolean = true): Unit = {
    val skip = (root.isInstanceOf[LeafExpression] && ignoreLeaf) ||
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      root.find(_.isInstanceOf[LambdaVariable]).isDefined
    // There are some special expressions that we should not recurse into children.
    //   1. CodegenFallback: it's children will not be used to generate code (call eval() instead)
    //   2. ReferenceToExpressions: it's kind of an explicit sub-expression elimination.
    val shouldRecurse = root match {
      // TODO: some expressions implements `CodegenFallback` but can still do codegen,
      // e.g. `CaseWhen`, we should support them.
      case _: CodegenFallback => false
      case _: ReferenceToExpressions if skipReferenceToExpressions => false
      case _ => true
    }
    if (!skip && !addExpr(root) && shouldRecurse) {
      root.children.foreach(addExprTree(_, ignoreLeaf))
    }
  }

  /**
   * Returns all of the expression trees that are equivalent to `e`. Returns
   * an empty collection if there are none.
   */
  def getEquivalentExprs(e: Expression): Seq[Expression] = {
    equivalenceMap.getOrElse(Expr(e), mutable.MutableList())
  }

  /**
   * Returns all the equivalent sets of expressions.
   */
  def getAllEquivalentExprs: Seq[Seq[Expression]] = {
    equivalenceMap.values.map(_.toSeq).toSeq
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb: mutable.StringBuilder = new StringBuilder()
    sb.append("Equivalent expressions:\n")
    equivalenceMap.foreach { case (k, v) =>
      if (all || v.length > 1) {
        sb.append("  " + v.mkString(", ")).append("\n")
      }
    }
    sb.toString()
  }
}
