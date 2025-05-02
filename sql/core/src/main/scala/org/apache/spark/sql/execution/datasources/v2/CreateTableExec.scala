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

import scala.jdk.CollectionConverters._

import org.apache.spark.internal.LogKeys.TABLE_NAME
import org.apache.spark.internal.MDC
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Column, Identifier, TableCatalog, TableInfo}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors

case class CreateTableExec(
    catalog: TableCatalog,
    identifier: Identifier,
    columns: Array[Column],
    partitioning: Seq[Transform],
    tableSpec: TableSpec,
    ignoreIfExists: Boolean) extends LeafV2CommandExec {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  val tableProperties = CatalogV2Util.convertTableProperties(tableSpec)

  override protected def run(): Seq[InternalRow] = {
    if (!catalog.tableExists(identifier)) {
      try {
        val tableInfo = new TableInfo.Builder()
          .withColumns(columns)
          .withPartitions(partitioning.toArray)
          .withProperties(tableProperties.asJava)
          .withConstraints(tableSpec.constraints.toArray)
          .build()
        catalog.createTable(identifier, tableInfo)
      } catch {
        case _: TableAlreadyExistsException if ignoreIfExists =>
          logWarning(
            log"Table ${MDC(TABLE_NAME, identifier.quoted)} was created concurrently. Ignoring.")
      }
    } else if (!ignoreIfExists) {
      throw QueryCompilationErrors.tableAlreadyExistsError(identifier)
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
