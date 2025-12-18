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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.variant.VariantGet
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{SEMI_STRUCTURED_EXTRACT, TreePattern}
import org.apache.spark.sql.types.{DataType, StringType, VariantType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Represents the extraction of data from a field that contains semi-structured data. The
 * semi-structured column can only be a Variant type for now.
 * @param child The semi-structured column
 * @param field The field to extract
 */
case class SemiStructuredExtract(
    child: Expression, field: String) extends UnaryExpression with Unevaluable {
  override lazy val resolved = false
  override def dataType: DataType = StringType

  final override val nodePatterns: Seq[TreePattern] = Seq(SEMI_STRUCTURED_EXTRACT)

  override protected def withNewChildInternal(newChild: Expression): SemiStructuredExtract =
    copy(child = newChild)
}

/**
 * Replaces SemiStructuredExtract expressions by extracting the specified field from the
 * semi-structured column (only VariantType is supported for now).
 */
case object ExtractSemiStructuredFields extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveExpressionsWithPruning(
    _.containsPattern(SEMI_STRUCTURED_EXTRACT), ruleId) {
    case SemiStructuredExtract(column, field) if column.resolved =>
      if (column.dataType.isInstanceOf[VariantType]) {
        VariantGet(column, Literal(UTF8String.fromString(field)), VariantType, failOnError = true)
      } else {
        throw new AnalysisException(
          errorClass = "COLUMN_IS_NOT_VARIANT_TYPE", messageParameters = Map.empty)
      }
  }
}
