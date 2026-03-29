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

package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.analysis.resolver.{LogicalPlanResolver, ResolverExtension}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * This is a [[ResolverExtension]] component that conditionally converts [[HiveTableRelation]]
 * to [[LogicalRelation]].
 */
class HiveTableRelationResolver(hiveCatalog: HiveSessionCatalog) extends ResolverExtension {
  private val relationConversions = RelationConversions(hiveCatalog)

  override def resolveOperator(
      operator: LogicalPlan,
      resolver: LogicalPlanResolver): Option[LogicalPlan] = {
    operator match {
      case hiveTableRelation: HiveTableRelation =>
        val result = if (relationConversions.doConvertHiveTableRelationForRead(hiveTableRelation)) {
          relationConversions.convertHiveTableRelationForRead(hiveTableRelation)
        } else {
          hiveTableRelation
        }

        Some(result)

      case _ =>
        None
    }
  }
}

/**
 * This is a [[ResolverExtension]] component that just passes through [[HiveTableRelation]] so that
 * [[Resolver]] can consider it being handled by an extension.
 */
class HiveTableRelationNoopResolver extends ResolverExtension {
  override def resolveOperator(
      operator: LogicalPlan,
      resolver: LogicalPlanResolver): Option[LogicalPlan] = {
    operator match {
      case hiveTableRelation: HiveTableRelation =>
        Some(hiveTableRelation)
      case _ =>
        None
    }
  }
}
