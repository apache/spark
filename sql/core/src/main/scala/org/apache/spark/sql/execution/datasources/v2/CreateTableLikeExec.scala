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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableChange}

/**
 * Physical plan node for altering a table.
 */
case class CreateTableLikeExec(
    targetCatalog: TableCatalog,
    targetTable: Identifier,
    sourceCatalog: TableCatalog,
    sourceTable: Identifier,
    ifNotExists: Boolean) extends V2CommandExec {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def output: Seq[Attribute] = Seq.empty

  override protected def run(): Seq[InternalRow] = {
    val sourceTab = sourceCatalog.loadTable(sourceTable)
    if (!targetCatalog.tableExists(targetTable)) {
      try {
        targetCatalog.createTable(targetTable,
          sourceTab.schema,
          sourceTab.partitioning,
          sourceTab.properties())
      } catch {
        case _: TableAlreadyExistsException if ifNotExists =>
          logWarning(s"Table ${targetTable.quoted} was created concurrently. Ignoring.")
      }
    } else if (!ifNotExists) {
      throw new TableAlreadyExistsException(targetTable)
    }

    Seq.empty
  }
}
