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

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CASE_WHEN, FILTER}
import org.apache.spark.sql.types.BooleanType


/**
 * A rule that eliminates CaseWhen conditional branches in FilterExec
 * which propagating an explicit value.
 * After this elimination, we can potentially alleviate the codegen complexity.
 */
object SimplifyConditionalsInFilter extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    val thresh = plan.conf.optimizerMaxConditionalsInFilter
    if (thresh >= 0) {
      plan.transformWithPruning(_.containsAllPatterns(FILTER, CASE_WHEN)) {
        case f @ Filter(c, _) if f.getMaxConditionalSize > thresh => //
          f.copy(condition = simplifyConditional(c))
      }
    } else plan
  }

  private def simplifyConditional(cond: Expression): Expression = cond match {
    case And(left, right) => And(simplifyConditional(left), simplifyConditional(right))
    case Or(left, right) => Or(simplifyConditional(left), simplifyConditional(right))
    case cls: Coalesce => simplifyCoalesce(cls)
    case cw: CaseWhen => simplifyCaseWhen(cw)
    case in @ In(value, list: Seq[Expression]) => doPropagate(in, value, list)
    case et @ EqualTo(left, right: Literal) => doPropagate(et, left, right)
    case e if e.dataType == BooleanType => e
    case e =>
      assert(e.dataType != BooleanType,
        "Expected a Boolean type expression in simplifyConditional, " +
          s"but got the type `${e.dataType.catalogString}` in `${e.sql}`.")
      e
  }

  // Make sure the semantics before and after branch elimination are equivalent.
  private def doPropagate(origin: In, //
                          value: Expression, list: Seq[Expression]): Expression = {
    if (list.exists(x => !x.isInstanceOf[Literal])) origin
    else {
      val anyExists = list.map(x => x.asInstanceOf[Literal])
      value match {
        case cls: Coalesce =>
          simplifyCoalesce(cls, anyExists)
        case cw: CaseWhen
          /* (_, _ @ (Some(Literal(null, NullType)) | None)) */ =>
          simplifyCaseWhen(cw, anyExists)
        case _ => origin
      }
    }
  }

  // Make sure the semantics before and after branch elimination are equivalent.
  private def doPropagate(origin: EqualTo, //
                          left: Expression, right: Literal): Expression = {
    left match {
      case cls: Coalesce =>
        simplifyCoalesce(cls, Seq(right))
      case cw: CaseWhen
        /* (_, _ @ (Some(Literal(null, NullType)) | None)) */ =>
        simplifyCaseWhen(cw, Seq(right))
      case _ => origin
    }
  }

  private def simplifyCoalesce(origin: Coalesce): Expression =
    Coalesce(children = origin.children.map {
      case cls: Coalesce => simplifyCoalesce(cls)
      case cw: CaseWhen => simplifyCaseWhen(cw)
      case e => simplifyConditional(e)
    })

  private def simplifyCoalesce(origin: Coalesce, anyExists: Seq[Literal]): Expression =
    Coalesce(children = origin.children.map {
      case cls: Coalesce => simplifyCoalesce(cls, anyExists)
      case cw: CaseWhen => simplifyCaseWhen(cw, anyExists)
      case e => simplifyValue(e, anyExists)
    })

  private def simplifyCaseWhen(origin: CaseWhen): Expression = {
    val tvb = doSimplify(origin.branches.map(b =>
      (simplifyConditional(b._1), simplifyConditional(b._2))))
    val tve = simplifyElse(origin.elseValue)

    collapseConditionals(tvb, tve)
  }

  private def simplifyCaseWhen(origin: CaseWhen, anyExists: Seq[Literal]): Expression = {
    val tvb = doSimplify(origin.branches.map(b => // convert to three-valued
      (simplifyConditional(b._1), simplifyValue(b._2, anyExists))))
    val tve = simplifyElse(origin.elseValue, anyExists)

    collapseConditionals(tvb, tve)
  }

  // Calcite always add null if the else branch not exists,
  // but catalyst can handle this well,
  // we'd better remove the superfluous efforts.
  private def simplifyElse(ev: Option[Expression]): Option[Expression] = ev match {
    case _ @ Some(v) => if (isNullLiteral(v)) Option.empty else Some(simplifyConditional(v))
    case e => e // None
  }

  // Calcite always add null if the else branch not exists,
  // but catalyst can handle this well,
  // we'd better remove the superfluous efforts.
  private def simplifyElse(ev: Option[Expression], //
                           anyExists: Seq[Literal]): Option[Expression] = ev match {
    case _ @ Some(v) => if (isNullLiteral(v)) Option.empty else Some(simplifyValue(v, anyExists))
    case e => e // None
  }

  // Convert to three-valued logic
  private def simplifyValue(expr: Expression, //
                            anyExists: Seq[Literal]): Expression = expr match {
    case lit: Literal =>
      // Meaningful for coalesce case.
      if (isNullLiteral(lit)) Literal.create(null, BooleanType)
      else if (anyExists.exists(x => x.semanticEquals(lit))) TrueLiteral
      else FalseLiteral
    case e => simplifyConditional(In(e, anyExists))
  }

  // 1. Eliminate duplicates;
  // 2. Merge conditionals;
  private def doSimplify(branches: Iterable[(Expression, Expression)]): //
  Iterable[(Expression, Expression)] = {
    if (branches.isEmpty) branches
    else {
      val after = ListBuffer[(Expression, Expression)]()
      for (b <- branches) {
        val condExists = after.exists(a => a._1.semanticEquals(b._1))
        if (!condExists) {
          var p = true
          var found = false
          val loop = new Breaks
          loop.breakable(for (i <- after.indices.reverse) {
            val a = after(i)
            p &= canMergeValue(a._1, b._1)
            if(!p) loop.break() // non-exclusive conditions
            else if (a._2.semanticEquals(b._2)) {
              val bc = doMergeValue(a._1, b._1)
              after.update(i, (bc, b._2))
              found = true
              loop.break()
            } /* else un-mergeable */
          })
          if (!found) {
            after += b
          }
        } // else duplicates
      }
      after.map {
        case (in @ In(e, l), v) =>
          if (l.size == 1) (EqualTo(e, l.head), v)
          else (in, v)
        case e => e
      }
    }
  }

  // Three-valued branches, else-value.
  private def collapseConditionals(tvb: Iterable[(Expression, Expression)],
                                   tve: Option[Expression]): Expression = {
    val after = tve.map { ev =>
      var stickOn = true
      val a = ListBuffer[(Expression, Expression)]()
      for (b <- tvb.toSeq.reverse) {
        if (stickOn) {
          if (!b._2.semanticEquals(ev)) {
            a += b
            stickOn = false
          }
        } else a += b
      }
      a.reverse
    }.getOrElse(tvb).map {
      case (c @ CaseWhen(branches, ev), v) =>
        ev.map { e =>
          if (canReachTrue(e)) (c, v)
          else (c.copy(branches, None), v)
        }.getOrElse((c, v))
      case e => e
    }.filter(b => canReachTrue(b._1)).toSeq // Eliminate meaningless branches.

    val allTrue = after.map(_._2).forall(_.semanticEquals(TrueLiteral))
    val allFalse = after.map(_._2).forall(_.semanticEquals(FalseLiteral))
    tve match {
      case Some(TrueLiteral) if allTrue => TrueLiteral
      case Some(FalseLiteral) if allTrue & after.size == 1 => after.head._1
      case Some(TrueLiteral) if allFalse & after.size == 1 => Not(after.head._1)
      case Some(FalseLiteral) if allFalse => FalseLiteral
      case Some(e) if after.isEmpty => e
      case None if after.isEmpty => Literal.create(null, BooleanType)
      case ev => CaseWhen(after, ev)
    }
  }

  private def canReachTrue(cond: Expression): Boolean = cond match {
    case And(left, right) =>
      canReachTrue(left) & canReachTrue(right)
    case Or(left, right) =>
      canReachTrue(left) | canReachTrue(right)
    case cls: Coalesce =>
      cls.children.exists(canReachTrue)
    case cw: CaseWhen =>
      cw.branches.exists(b => //
        canReachTrue(b._1) & canReachTrue(b._2)) | cw.elseValue.exists(canReachTrue)
    case lit: Literal => lit.semanticEquals(TrueLiteral)
    // other unknown condition default can reach
    case e if e.dataType == BooleanType => true
    case e =>
      assert(e.dataType != BooleanType,
        "Expected a Boolean type condition expression, " +
          s"but got the type `${e.dataType.catalogString}` in `${e.sql}`.")
      true
  }

  private def canMergeValue(a: Expression, b: Expression): Boolean = (a, b) match {
    case (EqualTo(al, _), EqualTo(bl, _)) => al.semanticEquals(bl)
    case (EqualTo(al, _), In(bv, _)) => al.semanticEquals(bv)
    case (In(av, _), EqualTo(bl, _)) => av.semanticEquals(bl)
    case (In(av, _), In(bv, _)) => av.semanticEquals(bv)
    case (_, _) => false
  }

  private def doMergeValue(a: Expression, b: Expression): Expression = (a, b) match {
    case (EqualTo(al, ar), EqualTo(_, br)) =>
      val r = (ar::br::Nil).distinct
      if (r.size == 1) EqualTo(al, ar)
      else In(al, r)
    case (In(av, la), EqualTo(_, br)) =>
      val r = (la :+ br).distinct
      if (r.size == 1) EqualTo(av, br)
      else In(av, r)
    case (EqualTo(al, ar), In(_, lb)) =>
      val r = (ar +: lb).distinct
      if (r.size == 1) EqualTo(al, ar)
      else In(al, r)
    case (In(av, la), In(_, lb)) =>
      val r = (la ++ lb).distinct
      if (r.size == 1) EqualTo(av, r.head)
      else In(av, r)
    }

  private def isNullLiteral(expr: Expression): Boolean = expr match {
    case Literal(null, _) => true
    case _ => false
  }

}
