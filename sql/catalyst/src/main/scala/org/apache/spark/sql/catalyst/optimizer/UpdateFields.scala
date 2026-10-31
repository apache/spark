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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{CommonExpressionRef, Expression, StructFieldsOperation, UpdateFields, UpdateNestedField, With, WithField}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{UPDATE_FIELDS, WITH_EXPRESSION}


/**
 * Optimizes [[UpdateFields]] expression chains.
 */
object OptimizeUpdateFields extends Rule[LogicalPlan] {
  private def canOptimize(names: Seq[String]): Boolean = {
    if (conf.caseSensitiveAnalysis) {
      names.distinct.length != names.length
    } else {
      names.map(_.toLowerCase(Locale.ROOT)).distinct.length != names.length
    }
  }

  private def optimizeFieldOps(
      fieldOps: Seq[StructFieldsOperation]): Seq[StructFieldsOperation] = {
    val operations = fieldOps.map {
      case nested: UpdateNestedField =>
        nested.copy(fieldOps = optimizeFieldOps(nested.fieldOps))
      case other => other
    }
    if (!operations.forall {
        case _: WithField | _: UpdateNestedField => true
        case _ => false
      }) {
      operations
    } else {
      val combined = mutable.LinkedHashMap.empty[String, StructFieldsOperation]
      val nestedChunks =
        mutable.Map.empty[String, mutable.ArrayBuffer[Seq[StructFieldsOperation]]]
      operations.foreach { operation =>
        val name = operation match {
          case WithField(name, _) => name
          case UpdateNestedField(name, _) => name
        }
        val key = if (conf.caseSensitiveAnalysis) name else name.toLowerCase(Locale.ROOT)
        val updated = operation match {
          case nested: UpdateNestedField => combined.get(key) match {
            case Some(previous: UpdateNestedField) =>
              nestedChunks.getOrElseUpdate(key,
                mutable.ArrayBuffer(previous.fieldOps)) += nested.fieldOps
              nested
            case Some(WithField(_, value)) =>
              WithField(name, UpdateFields(value, nested.fieldOps))
            case _ => nested
          }
          case other =>
            nestedChunks.remove(key)
            other
        }
        combined.update(key, updated)
      }
      combined.iterator.map {
        case (key, nested: UpdateNestedField) if nestedChunks.contains(key) =>
          val (prefix, tail) = nestedChunks(key).span(_.forall {
            case _: WithField | _: UpdateNestedField => true
            case _ => false
          })
          nested.copy(fieldOps =
            optimizeFieldOps(prefix.iterator.flatten.toVector) ++ tail.iterator.flatten.toVector)
        case (_, operation) => operation
      }.toSeq
    }
  }

  val optimizeUpdateFields: PartialFunction[Expression, Expression] = {
    case UpdateFields(structExpr, fieldOps)
      if fieldOps.forall(_.isInstanceOf[WithField]) &&
        canOptimize(fieldOps.map(_.asInstanceOf[WithField].name)) =>
      UpdateFields(structExpr, optimizeFieldOps(fieldOps))

    case UpdateFields(UpdateFields(struct, fieldOps1), fieldOps2) =>
      UpdateFields(struct, fieldOps1 ++ fieldOps2)

    case UpdateFields(struct, fieldOps)
        if fieldOps.exists(_.isInstanceOf[UpdateNestedField]) =>
      UpdateFields(struct, optimizeFieldOps(fieldOps))
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(
    _.containsPattern(UPDATE_FIELDS), ruleId)(optimizeUpdateFields)
}

/**
 * Replaces [[UpdateFields]] expression with an evaluable expression.
 */
object ReplaceUpdateFieldsExpression extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val replaced = plan.transformAllExpressionsWithPruning(_.containsPattern(UPDATE_FIELDS)) {
      case u: UpdateFields => u.evalExpr
    }
    // Lowering nested replacements can remove every reference to their struct input.
    replaced.transformWithPruning(_.containsPattern(WITH_EXPRESSION)) {
      case p => p.transformExpressionsUpWithPruning(_.containsPattern(WITH_EXPRESSION)) {
        case w @ With(child, defs) =>
          val referencedIds = child.collect { case ref: CommonExpressionRef => ref.id }.toSet
          if (defs.forall(d => !referencedIds.contains(d.id))) child else w
      }
    }
  }
}
