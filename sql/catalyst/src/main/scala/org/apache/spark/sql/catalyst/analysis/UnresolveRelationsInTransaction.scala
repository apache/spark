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

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, TransactionalWrite}
import org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.allowInvokingTransformsInAnalyzer
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * When a transaction is active, converts resolved [[DataSourceV2Relation]] nodes back to
 * [[V2TableReference]] placeholders for all relations loaded by a catalog with the same
 * name as the transaction catalog.
 *
 * This forces re-resolution of those relations against the transaction's catalog, which
 * intercepts [[TableCatalog#loadTable]] calls to track which tables are read as part of
 * the transaction.
 */
class UnresolveRelationsInTransaction(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {

  override def apply(plan: LogicalPlan): LogicalPlan =
    catalogManager.transaction match {
      case Some(transaction) =>
        // We use plain transform rather than resolveOperators* because the latter skips subtrees
        // that have already been analyzed. Furthermore, allowInvokingTransformsInAnalyzer
        // allows to suppress the assertNotAnalysisRule safety check, which forbids calling
        // transform directly inside the analyzer when not within a resolveOperators call.
        allowInvokingTransformsInAnalyzer {
          plan.transform {
            case tw: TransactionalWrite =>
              unresolveRelations(tw, transaction.catalog)
          }
        }
      case _ => plan
    }

  private def unresolveRelations(
      plan: LogicalPlan,
      catalog: CatalogPlugin): LogicalPlan = {
    plan.transform {
      case r: DataSourceV2Relation if isLoadedFromCatalog(r, catalog) =>
        V2TableReference.createForTransaction(r)
    }
  }

  private def isLoadedFromCatalog(
      relation: DataSourceV2Relation,
      catalog: CatalogPlugin): Boolean = {
    relation.catalog.exists(_.name == catalog.name) && relation.identifier.isDefined
  }
}
