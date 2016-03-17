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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}


class HiveSessionCatalog(hiveCatalog: HiveCatalog) extends SessionCatalog(hiveCatalog) {

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    val table = formatTableName(name.table)
    if (name.database.isDefined || !tempTables.containsKey(table)) {
      val newName = name.copy(table = table)
      hiveCatalog.lookupRelation(newName, alias)
    } else {
      val relation = tempTables.get(table)
      val tableWithQualifiers = SubqueryAlias(table, relation)
      // If an alias was specified by the lookup, wrap the plan in a subquery so that
      // attributes are properly qualified with this alias.
      alias.map(a => SubqueryAlias(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
    }
  }

}
