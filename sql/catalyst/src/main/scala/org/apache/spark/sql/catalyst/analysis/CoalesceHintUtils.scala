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

import java.util.Locale

import org.apache.spark.sql.catalyst.expressions.{Ascending, ByteLiteral, Expression, IntegerLiteral, ShortLiteral, SortOrder, StringLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Repartition, RepartitionByExpression, UnresolvedHint}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Helper functions used to build the logical plans for the "COALESCE", "REPARTITION",
 * "REPARTITION_BY_RANGE" and "REBALANCE" hints.
 *
 * These are pure functions that only depend on the provided [[UnresolvedHint]] (and the OSS
 * logical plan / expression classes), so they can be reused outside of the
 * [[ResolveHints.ResolveCoalesceHints]] rule.
 */
object CoalesceHintUtils {

  def getNumOfPartitions(hint: UnresolvedHint): (Option[Int], Seq[Expression]) = {
    hint.parameters match {
      case Seq(ByteLiteral(numPartitions), _*) =>
        (Some(numPartitions.toInt), hint.parameters.tail)
      case Seq(ShortLiteral(numPartitions), _*) =>
        (Some(numPartitions.toInt), hint.parameters.tail)
      case Seq(IntegerLiteral(numPartitions), _*) => (Some(numPartitions), hint.parameters.tail)
      case _ => (None, hint.parameters)
    }
  }

  def validateParameters(hint: String, parms: Seq[Expression]): Unit = {
    val invalidParams = parms.filter(!_.isInstanceOf[UnresolvedAttribute])
    if (invalidParams.nonEmpty) {
      val hintName = hint.toUpperCase(Locale.ROOT)
      throw QueryCompilationErrors.invalidHintParameterError(hintName, invalidParams)
    }
  }

  /**
   * This function handles hints for "COALESCE" and "REPARTITION".
   * The "COALESCE" hint only has a partition number as a parameter. The "REPARTITION" hint
   * has a partition number, columns, or both of them as parameters.
   */
  def createRepartition(shuffle: Boolean, hint: UnresolvedHint): LogicalPlan = {

    def createRepartitionByExpression(
        numPartitions: Option[Int], partitionExprs: Seq[Expression]): RepartitionByExpression = {
      val sortOrders = partitionExprs.filter(_.isInstanceOf[SortOrder])
      if (sortOrders.nonEmpty) {
        throw QueryCompilationErrors.invalidRepartitionExpressionsError(sortOrders)
      }
      validateParameters(hint.name, partitionExprs)
      RepartitionByExpression(partitionExprs, hint.child, numPartitions)
    }

    getNumOfPartitions(hint) match {
      case (Some(numPartitions), partitionExprs) if partitionExprs.isEmpty =>
        Repartition(numPartitions, shuffle, hint.child)
      // The "COALESCE" hint (shuffle = false) must have a partition number only
      case _ if !shuffle =>
        throw QueryCompilationErrors.invalidCoalesceHintParameterError(
          hint.name.toUpperCase(Locale.ROOT))
      case (Some(numPartitions), partitionExprs) =>
        createRepartitionByExpression(Some(numPartitions), partitionExprs)
      case (None, partitionExprs) =>
        createRepartitionByExpression(None, partitionExprs)
    }
  }

  /**
   * This function handles hints for "REPARTITION_BY_RANGE".
   * The "REPARTITION_BY_RANGE" hint must have column names and a partition number is optional.
   */
  def createRepartitionByRange(hint: UnresolvedHint): RepartitionByExpression = {
    def createRepartitionByExpression(
        numPartitions: Option[Int], partitionExprs: Seq[Expression]): RepartitionByExpression = {
      validateParameters(hint.name, partitionExprs)
      val sortOrder = partitionExprs.map {
        case expr: SortOrder => expr
        case expr: Expression => SortOrder(expr, Ascending)
      }
      RepartitionByExpression(sortOrder, hint.child, numPartitions)
    }

    getNumOfPartitions(hint) match {
      case (Some(numPartitions), partitionExprs) =>
        createRepartitionByExpression(Some(numPartitions), partitionExprs)
      case (None, partitionExprs) =>
        createRepartitionByExpression(None, partitionExprs)
    }
  }

  def transformStringToAttribute(hint: UnresolvedHint): UnresolvedHint = {
    // for all the coalesce hints, it's safe to transform the string literal to an attribute as
    // all the parameters should be column names.
    val parameters = hint.parameters.map {
      case StringLiteral(name) => UnresolvedAttribute(name)
      case e => e
    }
    hint.copy(parameters = parameters)
  }
}
