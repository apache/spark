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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for dropping a table.
 */
case class DropTableExec(
    catalog: TableCatalog,
    ident: Identifier,
    ifExists: Boolean,
    purge: Boolean,
    invalidateCache: () => Unit) extends LeafV2CommandExec {

  override def run(): Seq[InternalRow] = {
    if (catalog.tableExists(ident)) {
      invalidateCache()
      if (purge) catalog.purgeTable(ident) else catalog.dropTable(ident)
    } else if (!ifExists) {
      throw QueryCompilationErrors.noSuchTableError(
        (catalog.name() +: ident.namespace() :+ ident.name()).toImmutableArraySeq)
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
