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

import scala.collection.JavaConverters._

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{AlterTableUnsetProperties, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.TableCatalog

/**
 * A rule for resolving AlterTableUnsetProperties to handle non-existent properties.
 */
object ResolveTableProperties extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case a @ AlterTableUnsetProperties(r: ResolvedTable, props, ifExists) if !ifExists =>
      val tblProperties = r.table.properties.asScala
      props.foreach { p =>
        if (!tblProperties.contains(p) && p != TableCatalog.PROP_COMMENT) {
          throw new AnalysisException(
            s"Attempted to unset non-existent property '$p' in table '${r.identifier.quoted}'")
        }
      }
      a
  }
}
