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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.NamespaceHelper
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for SHOW VIEWS on a v2 [[ViewCatalog]]. Enumerates view identifiers via
 * [[ViewCatalog#listViews]]. v2 catalogs have no temp views, so the {@code isTemporary} column
 * is always false -- mirroring v1 {@code ShowViewsCommand}, which sets {@code isTemporary=true}
 * only for local/global temp views that live in the session catalog.
 */
case class ShowViewsExec(
    output: Seq[Attribute],
    catalog: ViewCatalog,
    namespace: Seq[String],
    pattern: Option[String]) extends V2CommandExec with LeafExecNode {
  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    catalog.listViews(namespace.toArray).foreach { ident =>
      val nameMatches =
        pattern.forall(p => StringUtils.filterPattern(Seq(ident.name), p).nonEmpty)
      if (nameMatches) {
        rows += toCatalystRow(ident.namespace().quoted, ident.name(), false)
      }
    }
    rows.toSeq
  }
}
