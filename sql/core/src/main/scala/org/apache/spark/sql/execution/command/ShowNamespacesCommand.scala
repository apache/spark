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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{CatalogHelper, NamespaceHelper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StringType

/**
 * The command for `SHOW NAMESPACES`.
 */
case class ShowNamespacesCommand(
    child: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowNamespacesCommand.output)
  extends UnaryRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val ResolvedNamespace(cat, ns, _) = child
    val nsCatalog = cat.asNamespaceCatalog
    val namespaces = if (ns.nonEmpty) {
      nsCatalog.listNamespaces(ns.toArray)
    } else {
      nsCatalog.listNamespaces()
    }

    // The legacy SHOW DATABASES command does not quote the database names.
    assert(output.length == 1)
    val namespaceNames = if (output.head.name == "databaseName"
      && namespaces.forall(_.length == 1)) {
      namespaces.map(_.head)
    } else {
      namespaces.map(_.quoted)
    }

    namespaceNames
      .filter{ns => pattern.forall(StringUtils.filterPattern(Seq(ns), _).nonEmpty)}
      .map(Row(_))
      .toSeq
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

object ShowNamespacesCommand {
  def output: Seq[AttributeReference] = {
    Seq(
      if (SQLConf.get.legacyOutputSchema) {
        AttributeReference("databaseName", StringType, nullable = false)()
      } else {
        AttributeReference("namespace", StringType, nullable = false)()
      }
    )
  }
}
