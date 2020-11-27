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
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.CatalogPlugin

/**
 * Physical plan node for dropping a namespace.
 */
case class DropNamespaceExec(
    catalog: CatalogPlugin,
    namespace: Seq[String],
    ifExists: Boolean,
    cascade: Boolean)
  extends V2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    val nsCatalog = catalog.asNamespaceCatalog
    val ns = namespace.toArray
    if (nsCatalog.namespaceExists(ns)) {
      // The default behavior of `SupportsNamespace.dropNamespace()` is cascading,
      // so make sure the namespace to drop is empty.
      if (!cascade) {
        if (catalog.asTableCatalog.listTables(ns).nonEmpty
          || nsCatalog.listNamespaces(ns).nonEmpty) {
          throw new SparkException(
            s"Cannot drop a non-empty namespace: ${namespace.quoted}. " +
              "Use CASCADE option to drop a non-empty namespace.")
        }
      }

      if (!nsCatalog.dropNamespace(ns)) {
        throw new SparkException(s"Failed to drop a namespace: ${namespace.quoted}.")
      }
    } else if (!ifExists) {
      throw new NoSuchNamespaceException(ns)
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
