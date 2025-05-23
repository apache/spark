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

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.NamespaceChange
import org.apache.spark.sql.connector.catalog.SupportsNamespaces.PROP_COLLATION

/**
 * The logical plan of the ALTER (DATABASE|SCHEMA|NAMESPACE) ... DEFAULT COLLATION command.
 */
case class SetNamespaceCollationCommand(
    namespace: LogicalPlan,
    collation: String) extends UnaryRunnableCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): SetNamespaceCollationCommand =
    copy(namespace = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ResolvedNamespace(catalog, ns, _) = child
    val collationChange = NamespaceChange.setProperty(PROP_COLLATION, collation)
    catalog.asNamespaceCatalog.alterNamespace(ns.toArray, collationChange)

    Seq.empty
  }
}
