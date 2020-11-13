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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog}

case class RefreshTableExec(
    session: SparkSession,
    catalog: TableCatalog,
    ident: Identifier) extends V2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    catalog.invalidateTable(ident)

    if (catalog.tableExists(ident)) {
      val table = catalog.loadTable(ident)
      // invalidate all caches referencing the given table
      // TODO(SPARK-33437): re-cache the table itself once we support caching a DSv2 table
      val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
      session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
