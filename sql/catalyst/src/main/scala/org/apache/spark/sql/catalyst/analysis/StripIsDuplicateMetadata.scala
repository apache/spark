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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreePattern.{ALIAS, ATTRIBUTE_REFERENCE}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder

/**
 * Strips is duplicate metadata from all attributes and aliases as it is no longer needed after
 * resolution.
 */
object StripIsDuplicateMetadata extends Rule[LogicalPlan] with SQLConfHelper {
  def apply(plan: LogicalPlan): LogicalPlan = AnalysisHelper.allowInvokingTransformsInAnalyzer {
    if (!conf.getConf(SQLConf.STRIP_IS_DUPLICATE_METADATA)) {
      plan
    } else {
      stripIsDuplicateMetadata(plan)
    }
  }

  private def stripIsDuplicateMetadata(plan: LogicalPlan) =
    plan.transformAllExpressionsWithPruning(_.containsAnyPattern(ALIAS, ATTRIBUTE_REFERENCE)) {
      case alias: Alias if alias.metadata.contains("__is_duplicate") =>
        val newMetadata = new MetadataBuilder()
          .withMetadata(alias.metadata)
          .remove("__is_duplicate")
          .build()

        val newAlias = CurrentOrigin.withOrigin(alias.origin) {
          Alias(child = alias.child, name = alias.name)(
            exprId = alias.exprId,
            qualifier = alias.qualifier,
            explicitMetadata = Some(newMetadata),
            nonInheritableMetadataKeys = alias.nonInheritableMetadataKeys
          )
        }
        newAlias.copyTagsFrom(alias)

        newAlias
      case attribute: Attribute if attribute.metadata.contains("__is_duplicate") =>
        val newMetadata = new MetadataBuilder()
          .withMetadata(attribute.metadata)
          .remove("__is_duplicate")
          .build()

        val newAttribute = CurrentOrigin.withOrigin(attribute.origin) {
          attribute.withMetadata(newMetadata = newMetadata)
        }
        newAttribute.copyTagsFrom(attribute)

        newAttribute
    }
}
