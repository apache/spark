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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, Attribute, AttributeMap, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NamedExpression, Not, Or}
import org.apache.spark.sql.catalyst.plans.logical.{Deduplicate, Distinct, Filter, LogicalPlan, Project, SerializeFromObject, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.trees.TreePattern.{FILTER, UNION}
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType}

/**
 * This rule optimizes Union operators by:
 * 1. Combines the children of [[Union]].
 * 2. Eliminate [[Union]] operators if all the children can be merged into one.
 */
object CombineUnionedSubquery extends Rule[LogicalPlan] with AliasHelper {

  /**
   * A tag to identify if the [[Union]] is the child of [[Distinct]] or [[Deduplicate]].
   */
  val UNION_ELIMINATE_DISABLED = TreeNodeTag[Unit]("union_eliminate_disabled")

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.combineUnionedAggregatesEnabled) return plan

    plan.transformDownWithPruning(
      _.containsAnyPattern(FILTER, UNION), ruleId) {
      case d @ Distinct(u: Union) =>
        u.setTagValue(UNION_ELIMINATE_DISABLED, ())
        d
      case d @ Deduplicate(_, u: Union) =>
        u.setTagValue(UNION_ELIMINATE_DISABLED, ())
        d
      case u: Union if u.getTagValue(UNION_ELIMINATE_DISABLED).isEmpty => eliminateUnion(u)
    }
  }

  private def eliminateUnion(union: Union): LogicalPlan = {
    val cache = mutable.ArrayBuffer.empty[LogicalPlan]
    union.children.foreach(subPlan => mergeSubPlan(subPlan, cache))

    assert(cache.size > 0)

    if (cache.size == union.children.size) {
      union
    } else if (cache.size > 1) {
      union.copy(children = cache.toSeq)
    } else {
      cache.head
    }
  }

  private def mergeSubPlan(subPlan: LogicalPlan, cache: mutable.ArrayBuffer[LogicalPlan]): Unit = {
    cache.zipWithIndex.collectFirst(Function.unlift {
      case (cachedPlan, subqueryIndex) if subPlan.canonicalized != cachedPlan.canonicalized =>
        tryMergePlans(subPlan, cachedPlan).map {
          case (mergedPlan, _) =>
            cache(subqueryIndex) = mergedPlan
        }
      case _ => None
    }).getOrElse {
      cache += subPlan
    }
  }

  // Recursively traverse down and try merging 2 plans. If merge is possible then return the merged
  // plan with the attribute mapping from the new to the merged version.
  private def tryMergePlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[(LogicalPlan, AttributeMap[Attribute])] = {
    checkIdenticalPlans(newPlan, cachedPlan).map(cachedPlan -> _).orElse(
      (newPlan, cachedPlan) match {
        case (np: Project, cp: Project) =>
          tryMergePlans(np.child, cp.child).flatMap { case (mergedChild, outputMap) =>
            val projectTuples = np.projectList.map { ne =>
              val mapped = mapAttributes(ne, outputMap)
              ne.canonicalized -> mapped
            }

            if (checkIdenticalProjectList(projectTuples.map(_._2), cp.projectList)) {
              val projectMap = projectTuples.toMap
              val mergedProjectList = mutable.ArrayBuffer[NamedExpression](cp.projectList: _*)
              val newOutputMap = AttributeMap(np.projectList.map { ne =>
                val mapped = projectMap(ne.canonicalized)
                val withoutAlias = mapped match {
                  case Alias(child, _) => child
                  case e => e
                }
                ne.toAttribute -> mergedProjectList.find {
                  case Alias(child, _) => child semanticEquals withoutAlias
                  case e => e semanticEquals withoutAlias
                }.getOrElse {
                  mergedProjectList += mapped
                  mapped
                }.toAttribute
              })
              val mergedPlan = Project(mergedProjectList.toSeq, mergedChild)
              Some(mergedPlan -> newOutputMap)
            } else {
              None
            }
          }

        case (nf: Filter, cf: Filter) =>
          tryMergePlans(nf.child, cf.child).flatMap { case (mergedChild, outputMap) =>
            val mappedNewCondition = mapAttributes(nf.condition, outputMap)
            if (checkCondition(mappedNewCondition, cf.condition)) {
              val combinedCondition = Or(cf.condition, mappedNewCondition)
              val mergedPlan = cf.copy(condition = combinedCondition, child = mergedChild)
              Some(mergedPlan -> outputMap)
            } else {
              None
            }
          }

        case (ns: SerializeFromObject, cs: SerializeFromObject) =>
          checkIdenticalPlans(ns, cs).map { outputMap =>
            (cs, outputMap)
          }

        case _ => None
      }
    )
  }

  private def checkIdenticalProjectList(
      nes: Seq[NamedExpression], ces: Seq[NamedExpression]): Boolean = {
    val npAliases = getAliasMap(nes)
    val cpAliases = getAliasMap(ces)
    nes.zip(ces).forall {
      case (ne1, ne2) =>
        replaceAlias(ne1, npAliases).semanticEquals(replaceAlias(ne2, cpAliases))
    }
  }

  private def checkCondition(leftCondition: Expression, rightCondition: Expression): Boolean = {
    val normalizedLeft = normalizeExpression(leftCondition)
    val normalizedRight = normalizeExpression(rightCondition)
    if (normalizedLeft.isDefined && normalizedRight.isDefined) {
      (normalizedLeft.get, normalizedRight.get) match {
        case (a GreaterThan b, c LessThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(b, d, a.dataType)
        case (a LessThan b, c GreaterThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(d, b, a.dataType)
        case (a GreaterThanOrEqual b, c LessThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(b, d, a.dataType)
        case (a LessThan b, c GreaterThanOrEqual d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(d, b, a.dataType)
        case (a GreaterThan b, c LessThanOrEqual d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(b, d, a.dataType)
        case (a LessThanOrEqual b, c GreaterThan d) if a.semanticEquals(c) =>
          isGreaterOrEqualTo(d, b, a.dataType)
        case (a EqualTo b, Not(c EqualTo d)) if a.semanticEquals(c) =>
          isEqualTo(b, d, a.dataType)
        case _ => false
      }
    } else {
      false
    }
  }

  private def normalizeExpression(expr: Expression): Option[Expression] = {
    expr match {
      case gt @ GreaterThan(_, r) if r.foldable =>
        Some(gt)
      case l GreaterThan r if l.foldable =>
        Some(LessThanOrEqual(r, l))
      case lt @ LessThan(_, r) if r.foldable =>
        Some(lt)
      case l LessThan r if l.foldable =>
        Some(GreaterThanOrEqual(r, l))
      case gte @ GreaterThanOrEqual(_, r) if r.foldable =>
        Some(gte)
      case l GreaterThanOrEqual r if l.foldable =>
        Some(LessThan(r, l))
      case lte @ LessThanOrEqual(_, r) if r.foldable =>
        Some(lte)
      case l LessThanOrEqual r if l.foldable =>
        Some(GreaterThan(r, l))
      case eq @ EqualTo(_, r) if r.foldable =>
        Some(eq)
      case l EqualTo r if l.foldable =>
        Some(EqualTo(r, l))
      case not @ Not(EqualTo(l, r)) if r.foldable =>
        Some(not)
      case Not(l EqualTo r) if l.foldable =>
        Some(Not(EqualTo(r, l)))
      case _ => None
    }
  }

  private def isGreaterOrEqualTo(
      left: Expression, right: Expression, dataType: DataType): Boolean = dataType match {
    case ShortType => left.eval().asInstanceOf[Short] >= right.eval().asInstanceOf[Short]
    case IntegerType => left.eval().asInstanceOf[Int] >= right.eval().asInstanceOf[Int]
    case LongType => left.eval().asInstanceOf[Long] >= right.eval().asInstanceOf[Long]
    case FloatType => left.eval().asInstanceOf[Float] >= right.eval().asInstanceOf[Float]
    case DoubleType => left.eval().asInstanceOf[Double] >= right.eval().asInstanceOf[Double]
    case DecimalType.Fixed(_, _) =>
      left.eval().asInstanceOf[Decimal] >= right.eval().asInstanceOf[Decimal]
    case _ => false
  }

  private def isEqualTo(
      left: Expression, right: Expression, dataType: DataType): Boolean = dataType match {
    case ShortType => left.eval().asInstanceOf[Short] == right.eval().asInstanceOf[Short]
    case IntegerType => left.eval().asInstanceOf[Int] == right.eval().asInstanceOf[Int]
    case LongType => left.eval().asInstanceOf[Long] == right.eval().asInstanceOf[Long]
    case FloatType => left.eval().asInstanceOf[Float] == right.eval().asInstanceOf[Float]
    case DoubleType => left.eval().asInstanceOf[Double] == right.eval().asInstanceOf[Double]
    case DecimalType.Fixed(_, _) =>
      left.eval().asInstanceOf[Decimal] == right.eval().asInstanceOf[Decimal]
    case _ => false
  }

  private def checkIdenticalPlans(
      newPlan: LogicalPlan,
      cachedPlan: LogicalPlan): Option[AttributeMap[Attribute]] = {
    if (newPlan.canonicalized == cachedPlan.canonicalized) {
      Some(AttributeMap(newPlan.output.zip(cachedPlan.output)))
    } else {
      None
    }
  }

  private def mapAttributes[T <: Expression](expr: T, outputMap: AttributeMap[Attribute]) = {
    expr.transform {
      case a: Attribute => outputMap.getOrElse(a, a)
    }.asInstanceOf[T]
  }
}
