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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, CodegenFallback}
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

  /**
   * Wrapper around an Expression that provides structural semantic equality.
   */
  case class StructuralExpr(e: Expression) {
    def normalized(expr: Expression): Expression = {
      expr.transformUp {
        case b: ParameterizedBoundReference =>
          b.copy(parameter = "")
      }
    }
    override def equals(o: Any): Boolean = o match {
      case other: StructuralExpr =>
        normalized(e).semanticEquals(normalized(other.e))
      case _ => false
    }

    override def hashCode: Int = normalized(e).semanticHash()
  }

  type EquivalenceMap = mutable.HashMap[Expr, mutable.ArrayBuffer[Expression]]

  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[Expr, mutable.ArrayBuffer[Expression]]

  // For each expression, the set of structurally equivalent expressions.
  private val structEquivalenceMap = mutable.HashMap.empty[StructuralExpr, EquivalenceMap]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression, exprMap: EquivalenceMap = this.equivalenceMap): Boolean = {
    if (expr.deterministic) {
      val e: Expr = Expr(expr)
      val f = exprMap.get(e)
      if (f.isDefined) {
        f.get += expr
        true
      } else {
        exprMap.put(e, mutable.ArrayBuffer(expr))
        false
      }
    } else {
      false
    }
  }

  /**
   * Adds each expression to structural expression data structure, grouping them with existing
   * structurally equivalent expressions. Non-recursive.
   */
  def addStructExpr(ctx: CodegenContext, expr: Expression): Unit = {
    if (expr.deterministic) {
      val refs = expr.collect {
        case b: BoundReference => b
      }

      // For structural equivalent expressions, we need to pass in int type ordinals into
      // split functions. If the number of ordinals is more than JVM function limit, we skip
      // this expression.
      // We calculate function parameter length by the number of ints plus `INPUT_ROW` plus
      // a int type result array index.
      val parameterLength = CodeGenerator.calculateParamLength(refs.map(_ => Literal(0))) + 2
      if (CodeGenerator.isValidParamLength(parameterLength)) {
        val parameterizedExpr = parameterizedBoundReferences(ctx, expr)

        val e: StructuralExpr = StructuralExpr(parameterizedExpr)
        val f = structEquivalenceMap.get(e)
        if (f.isDefined) {
          addExpr(expr, f.get)
        } else {
          val exprMap = mutable.HashMap.empty[Expr, mutable.ArrayBuffer[Expression]]
          addExpr(expr, exprMap)
          structEquivalenceMap.put(e, exprMap)
        }
      }
    }
  }

  /**
   * Replaces bound references in given expression by parameterized bound references.
   */
  private def parameterizedBoundReferences(ctx: CodegenContext, expr: Expression): Expression = {
    expr.transformUp {
      case b: BoundReference =>
        val param = ctx.freshName("boundInput")
        ParameterizedBoundReference(param, b.dataType, b.nullable)
    }
  }

  /**
   * Checks if we skip add sub-expressions for given expression.
   */
  private def skipExpr(expr: Expression): Boolean = {
    expr.isInstanceOf[LeafExpression] ||
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      expr.find(_.isInstanceOf[LambdaVariable]).isDefined
  }


  // There are some special expressions that we should not recurse into all of its children.
  //   1. CodegenFallback: it's children will not be used to generate code (call eval() instead)
  //   2. If: common subexpressions will always be evaluated at the beginning, but the true and
  //          false expressions in `If` may not get accessed, according to the predicate
  //          expression. We should only recurse into the predicate expression.
  //   3. CaseWhen: like `If`, the children of `CaseWhen` only get accessed in a certain
  //                condition. We should only recurse into the first condition expression as it
  //                will always get accessed.
  //   4. Coalesce: it's also a conditional expression, we should only recurse into the first
  //                children, because others may not get accessed.
  private def childrenToRecurse(expr: Expression): Seq[Expression] = expr match {
    case _: CodegenFallback => Nil
    case i: If => i.predicate :: Nil
    case c: CaseWhen => c.children.head :: Nil
    case c: Coalesce => c.children.head :: Nil
    case s: SortPrefix => s.child.child :: Nil
    case other => other.children
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
   */
  def addExprTree(
      expr: Expression,
      exprMap: EquivalenceMap = this.equivalenceMap): Unit = {
    val skip = skipExpr(expr)

    if (!skip && !addExpr(expr, exprMap)) {
      childrenToRecurse(expr).foreach(addExprTree(_, exprMap))
    }
  }

  /**
   * Adds the expression to structural data structure recursively.  Stops if a matching expression
   * is found.
   */
  def addStructuralExprTree(ctx: CodegenContext, expr: Expression): Unit = {
    val skip = skipExpr(expr) || expr.isInstanceOf[CodegenFallback]

    if (!skip) {
      addStructExpr(ctx, expr)
      childrenToRecurse(expr).foreach(addStructuralExprTree(ctx, _))
    }
  }

  /**
   * Returns all of the expression trees that are equivalent to `e`. Returns
   * an empty collection if there are none.
   */
  def getEquivalentExprs(e: Expression): Seq[Expression] = {
    equivalenceMap.getOrElse(Expr(e), Seq.empty)
  }

  /**
   * Returns all the equivalent sets of expressions.
   */
  def getAllEquivalentExprs: Seq[Seq[Expression]] = {
    equivalenceMap.values.map(_.toSeq).toSeq
  }

  def getStructurallyEquivalentExprs(ctx: CodegenContext, e: Expression): Seq[Seq[Expression]] = {
    val parameterizedExpr = parameterizedBoundReferences(ctx, e)

    val key = StructuralExpr(parameterizedExpr)
    structEquivalenceMap.get(key).map(_.values.map(_.toSeq).toSeq).getOrElse(Seq.empty)
  }

  /**
   * Returns all the structurally equivalent sets of expressions.
   */
  def getAllStructuralExpressions: Map[StructuralExpr, EquivalenceMap] = {
    structEquivalenceMap.toMap
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
