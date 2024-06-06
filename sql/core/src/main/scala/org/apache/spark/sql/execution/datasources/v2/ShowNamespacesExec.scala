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
import org.apache.spark.sql.connector.catalog.SupportsNamespaces
import org.apache.spark.sql.execution.LeafExecNode

/**
 * Physical plan node for showing namespaces.
 */
case class ShowNamespacesExec(
    output: Seq[Attribute],
    catalog: SupportsNamespaces,
    namespace: Seq[String],
    pattern: Option[String]) extends V2CommandExec with LeafExecNode {

  override protected def run(): Seq[InternalRow] = {
    val namespaces = if (namespace.nonEmpty) {
      catalog.listNamespaces(namespace.toArray)
    } else {
      catalog.listNamespaces()
    }

    // Please refer to the rule `KeepLegacyOutputs` for details about legacy command.
    // The legacy SHOW DATABASES command does not quote the database names.
    val isLegacy = output.head.name == "databaseName"
    val namespaceNames = if (isLegacy && namespaces.forall(_.length == 1)) {
      namespaces.map(_.head)
    } else {
      namespaces.map(_.quoted)
    }

    val rows = new ArrayBuffer[InternalRow]()
    namespaceNames.map { ns =>
      if (pattern.map(StringUtils.filterPattern(Seq(ns), _).nonEmpty).getOrElse(true)) {
        rows += toCatalystRow(ns)
      }
    }

    rows.toSeq
  }
}
