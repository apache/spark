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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.analysis.resolver.{
  ExplicitlyUnsupportedResolverFeature,
  ResolverExtension
}
import org.apache.spark.sql.catalyst.catalog.UnresolvedCatalogRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.streaming.StreamingRelation

/**
 * The [[DataSourceResolver]] is a [[Resolver]] extension that resolves nodes defined in the
 * [[datasources]] package. We have it as an extension to avoid cyclic dependencies between
 * [[resolver]] and [[datasources]] packages.
 */
class DataSourceResolver(sparkSession: SparkSession) extends ResolverExtension {
  private val findDataSourceTable = new FindDataSourceTable(sparkSession)

  /**
   * Resolve [[UnresolvedCatalogRelation]]:
   * - Reuse [[FindDataSourceTable]] code to resolve [[UnresolvedCatalogRelation]]
   * - Create a new instance of [[LogicalRelation]] to regenerate the expression IDs
   * - Explicitly disallow [[StreamingRelation]] and [[StreamingRelationV2]] for now
   * - [[FileResolver]], which is a [[ResolverExtension]], introduces a new [[LogicalPlan]] node
   *    which resolution has to be handled here (further resolution of it doesn't need any specific
   *    resolution except adding it's attributes to the scope).
   */
  override def resolveOperator: PartialFunction[LogicalPlan, LogicalPlan] = {
    case unresolvedCatalogRelation: UnresolvedCatalogRelation =>
      val result = findDataSourceTable.resolveUnresolvedCatalogRelation(unresolvedCatalogRelation)
      result match {
        case logicalRelation: LogicalRelation =>
          logicalRelation.newInstance()
        case streamingRelation: StreamingRelation =>
          throw new ExplicitlyUnsupportedResolverFeature(
            s"unsupported operator: ${streamingRelation.getClass.getName}"
          )
        case streamingRelationV2: StreamingRelationV2 =>
          throw new ExplicitlyUnsupportedResolverFeature(
            s"unsupported operator: ${streamingRelationV2.getClass.getName}"
          )
        case other =>
          other
      }
    case logicalRelation: LogicalRelation =>
      logicalRelation.newInstance()
  }
}
