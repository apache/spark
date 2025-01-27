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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{DataSourceResolver, LogicalRelation}

/**
 * [[DataSourceWithHiveResolver]] is a [[DataSourceResolver]] that additionally handles
 * [[HiveTableRelation]] conversion using [[RelationConversions]].
 */
class DataSourceWithHiveResolver(sparkSession: SparkSession, hiveCatalog: HiveSessionCatalog)
    extends DataSourceResolver(sparkSession) {
  private val relationConversions = RelationConversions(hiveCatalog)

  /**
   * Invoke [[DataSourceResolver]] to resolve the input operator. If [[DataSourceResolver]] produces
   * [[HiveTableRelation]], convert it to [[LogicalRelation]] if possible.
   */
  override def resolveOperator: PartialFunction[LogicalPlan, LogicalPlan] = {
    case operator: LogicalPlan if super.resolveOperator.isDefinedAt(operator) =>
      val relationAfterDataSourceResolver = super.resolveOperator(operator)

      relationAfterDataSourceResolver match {
        case hiveTableRelation: HiveTableRelation =>
          resolveHiveTableRelation(hiveTableRelation)
        case other => other
      }
  }

  private def resolveHiveTableRelation(hiveTableRelation: HiveTableRelation): LogicalPlan = {
    if (relationConversions.doConvertHiveTableRelationForRead(hiveTableRelation)) {
      val logicalRelation: LogicalRelation =
        relationConversions.convertHiveTableRelationForRead(hiveTableRelation)
      logicalRelation.newInstance()
    } else {
      hiveTableRelation.newInstance()
    }
  }
}
