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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Cast, CustomPredicateExpression, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CustomPredicateDescriptor, SupportsCustomPredicates}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves function calls against custom predicates declared by DSv2 tables.
 * Matches unresolved function names against the sqlName of each descriptor.
 *
 * This rule must run before ResolveFunctions in the Resolution batch, because
 * ResolveFunctions will throw on unknown function names.
 */
object ResolveCustomPredicates extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.extendedPredicatePushdownEnabled) return plan
    plan.resolveOperatorsUp {
    case f @ Filter(condition, child) if hasUnresolvedFunctions(condition) =>
      collectCustomPredicates(child) match {
        case Some(descriptors) if descriptors.nonEmpty =>
          val resolved = condition.transformUp {
            case u: UnresolvedFunction if u.nameParts.length == 1 =>
              findDescriptor(descriptors, u.nameParts.head) match {
                case Some(descriptor) =>
                  val args = if (descriptor.parameterTypes() != null &&
                      descriptor.parameterTypes().length > 0 &&
                      descriptor.parameterTypes().length == u.arguments.length) {
                    castArgumentsIfNeeded(u.arguments, descriptor)
                  } else {
                    u.arguments
                  }
                  CustomPredicateExpression(descriptor, args)
                case None => u
              }
          }
          Filter(resolved, child)
        case _ => f
      }
  }
  }

  private def hasUnresolvedFunctions(expr: Expression): Boolean = {
    expr.exists(_.isInstanceOf[UnresolvedFunction])
  }

  /**
   * Collects custom predicate descriptors from the first DSv2 relation found.
   * Note: only the first relation's descriptors are collected. Multi-table queries
   * (e.g. joins) where different tables declare different custom predicates are
   * not currently supported -- users should use function-call syntax with explicit
   * table references in such cases.
   */
  private def collectCustomPredicates(
      plan: LogicalPlan): Option[Array[CustomPredicateDescriptor]] = {
    plan.collectFirst {
      case r: DataSourceV2Relation =>
        r.table match {
          case t: SupportsCustomPredicates => t.customPredicates()
          case _ => Array.empty[CustomPredicateDescriptor]
        }
    }
  }

  private def findDescriptor(
      descriptors: Array[CustomPredicateDescriptor],
      name: String): Option[CustomPredicateDescriptor] = {
    descriptors.find(_.sqlName().equalsIgnoreCase(name))
  }

  private def castArgumentsIfNeeded(
      args: Seq[Expression],
      descriptor: CustomPredicateDescriptor): Seq[Expression] = {
    val paramTypes = descriptor.parameterTypes()
    args.zipWithIndex.map { case (arg, i) =>
      if (i < paramTypes.length && paramTypes(i) != null &&
          arg.resolved && arg.dataType != paramTypes(i)) {
        Cast(arg, paramTypes(i))
      } else {
        arg
      }
    }
  }
}
