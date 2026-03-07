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

import scala.collection.mutable.ArrayBuffer

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
 * Custom predicates are only valid in Filter (WHERE) conditions. If a custom
 * predicate name appears in a non-filter context (e.g. SELECT list), it is
 * left unresolved so that ResolveFunctions will report a clear error.
 *
 * This rule must run before ResolveFunctions in the Resolution batch, because
 * ResolveFunctions will throw on unknown function names.
 */
object ResolveCustomPredicates extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!SQLConf.get.extendedPredicatePushdownEnabled) return plan
    plan.resolveOperatorsUp {
    case f @ Filter(condition, child) if hasUnresolvedFunctions(condition) =>
      val descriptors = collectCustomPredicates(child)
      if (descriptors.nonEmpty) {
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
      } else {
        f
      }
  }
  }

  private def hasUnresolvedFunctions(expr: Expression): Boolean = {
    expr.exists(_.isInstanceOf[UnresolvedFunction])
  }

  /**
   * Collects custom predicate descriptors from all DSv2 relations in the plan.
   * Descriptors from multiple tables are merged so that multi-table queries
   * (e.g. joins) can reference custom predicates from any participating table.
   */
  private def collectCustomPredicates(
      plan: LogicalPlan): Array[CustomPredicateDescriptor] = {
    val descriptors = new ArrayBuffer[CustomPredicateDescriptor]()
    plan.foreach {
      case r: DataSourceV2Relation =>
        r.table match {
          case t: SupportsCustomPredicates =>
            descriptors ++= t.customPredicates()
          case _ =>
        }
      case _ =>
    }
    descriptors.toArray
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
