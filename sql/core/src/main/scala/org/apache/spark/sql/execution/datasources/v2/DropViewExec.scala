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
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, ViewCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical plan node for DROP VIEW on a v2 [[ViewCatalog]]. Calls [[ViewCatalog#dropView]]; if
 * it returns false and the catalog also implements [[TableCatalog]] with a table at this
 * identifier, surfaces `WRONG_COMMAND_FOR_OBJECT_TYPE` ("Use DROP TABLE instead") rather than
 * a generic "view not found" -- matching v1 `DropTableCommand(isView = true)`.
 */
case class DropViewExec(
    catalog: ViewCatalog,
    ident: Identifier,
    ifExists: Boolean,
    invalidateCache: () => Unit) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    val dropped = catalog.dropView(ident)
    if (dropped) {
      invalidateCache()
    } else {
      val nameParts =
        (catalog.name() +: ident.namespace() :+ ident.name()).toImmutableArraySeq
      catalog match {
        case tc: TableCatalog if tc.tableExists(ident) =>
          throw QueryCompilationErrors.wrongCommandForObjectTypeError(
            operation = "DROP VIEW",
            requiredType = "VIEW",
            objectName = nameParts.map(quoteIfNeeded).mkString("."),
            foundType = "TABLE",
            alternative = "DROP TABLE")
        case _ if !ifExists =>
          throw new NoSuchViewException(ident)
        case _ =>
        // IF EXISTS: no-op.
      }
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
