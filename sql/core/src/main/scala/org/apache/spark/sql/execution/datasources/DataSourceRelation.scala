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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType


/**
 * A trait that should be mixed into a data source. It provides a set of methods and fields
 * to convert the data source to be analyzed, for example, from a data source to
 * `DataFrame` or [[LogicalPlan]].
 *
 * @note fields and methods here are intentionally `private[spark]` to prevent
 * access from inherited interface.
 */
trait DataSourceRelation {
  import DataSourceRelation._

  /**
   * Convert a data source created for external data sources into a `DataFrame`.
   */
  private[spark] final def toDataFrame(sparkSession: SparkSession): DataFrame = this match {
    case dsV1: DataSourceV1Relation => sparkSession.baseRelationToDataFrame(dsV1)
    case dsV2: DataSourceV2Relation => Dataset.ofRows(sparkSession, dsV2)
    case _ => throw new AnalysisException(s"Unexpected relation $this.")
  }

  /**
   * Convert a data source created for external data sources into a [[LogicalPlan]].
   */
  private[spark] final def toLogicalPlan: LogicalPlan = this match {
    case dsV1: DataSourceV1Relation => LogicalRelation(dsV1)
    case dsV2: DataSourceV2Relation => dsV2
    case _ => throw new AnalysisException(s"Unexpected relation $this.")
  }

  private[spark] final def toLogicalPlan(isStreaming: Boolean): LogicalPlan = this match {
    case dsV1: DataSourceV1Relation if isStreaming => LogicalRelation(dsV1, isStreaming)
    // TODO: Add StreamingDataSourceV2Relation as well.
    case _ => toLogicalPlan
  }

  private[spark] final def toLogicalPlan(table: Option[CatalogTable]): LogicalPlan = {
    this match {
      case dsV1: DataSourceV1Relation if table.isDefined => LogicalRelation(dsV1, table.get)
      case dsV2: DataSourceV2Relation if table.isDefined =>
        dsV2.copy(tableIdent = table.map(_.identifier))
      case _ => toLogicalPlan
    }
  }

  private[spark] final def sourceSchema: StructType = this match {
    case dsV1: DataSourceV1Relation => dsV1.schema
    case dsV2: DataSourceV2Relation => dsV2.schema
    case _ => throw new AnalysisException(s"Unexpected relation $this.")
  }
}

object DataSourceRelation {
  private type DataSourceV1Relation = BaseRelation

  def newOutputCopy(plan: LogicalPlan, newOutput: Seq[AttributeReference]): LogicalPlan = {
    plan match {
      case relationV1: LogicalRelation => relationV1.copy(output = newOutput)
      case relationV2: DataSourceV2Relation => relationV2.copy(output = newOutput)
      case _ => throw new AnalysisException(s"Unexpected relation $plan.")
    }
  }
}
