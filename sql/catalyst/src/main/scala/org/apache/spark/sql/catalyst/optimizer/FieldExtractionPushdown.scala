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

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, GetStructField}
import org.apache.spark.sql.catalyst.planning.SelectedField
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

abstract class FieldExtractionPushdown extends Rule[LogicalPlan] {
  final override def apply(plan: LogicalPlan): LogicalPlan =
    if (SQLConf.get.nestedSchemaPruningEnabled) {
      apply0(plan)
    } else {
      plan
    }

  protected def apply0(plan: LogicalPlan): LogicalPlan

  // Gets the top-level GetStructField expressions from the given expression
  // and its children. This does not return children of a GetStructField.
  protected final def getFieldExtractors(expr: Expression): Seq[GetStructField] =
    expr match {
      // Check that getField matches SelectedField(_) to ensure that getField defines a chain of
      // extractors down to an attribute.
      case getField: GetStructField if SelectedField.unapply(getField).isDefined =>
        getField :: Nil
      case _ =>
        expr.children.flatMap(getFieldExtractors)
    }

  // Constructs aliases and a substitution function for the given sequence of
  // GetStructField expressions.
  protected final def constructAliasesAndSubstitutions(fieldExtractors: Seq[GetStructField]) = {
    val aliases =
      fieldExtractors.map(extractor =>
        Alias(extractor, extractor.childSchema(extractor.ordinal).name)())

    val attributes = aliases.map(alias => (alias.child, alias.toAttribute)).toMap

    val substituteAttributes: Expression => Expression = _.transformDown {
      case expr: GetStructField => attributes.getOrElse(expr, expr)
    }

    (aliases, substituteAttributes)
  }
}
