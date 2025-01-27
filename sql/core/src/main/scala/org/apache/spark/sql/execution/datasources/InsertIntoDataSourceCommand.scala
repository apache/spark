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
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{CTEInChildren, CTERelationDef, LogicalPlan, WithCTE}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.sources.InsertableRelation


/**
 * Inserts the results of `query` in to a relation that extends [[InsertableRelation]].
 */
case class InsertIntoDataSourceCommand(
    logicalRelation: LogicalRelation,
    query: LogicalPlan,
    overwrite: Boolean)
  extends LeafRunnableCommand with CTEInChildren {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val relation = logicalRelation.relation.asInstanceOf[InsertableRelation]
    val data = Dataset.ofRows(sparkSession, query)
    // Data has been casted to the target relation's schema by the PreprocessTableInsertion rule.
    relation.insert(data, overwrite)

    // Re-cache all cached plans(including this relation itself, if it's cached) that refer to this
    // data source relation.
    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, logicalRelation)

    Seq.empty[Row]
  }

  override def withCTEDefs(cteDefs: Seq[CTERelationDef]): LogicalPlan = {
    copy(query = WithCTE(query, cteDefs))
  }
}
