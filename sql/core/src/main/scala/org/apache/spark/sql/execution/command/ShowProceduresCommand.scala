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
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.types.{ArrayType, StringType}

/**
 * A command for users to get procedures.
 * If a namespace is not given, the current namespace will be used.
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW PROCEDURES [(IN|FROM) namespace]]
 * }}}
 */
case class ShowProceduresCommand(
    child: LogicalPlan,
    override val output: Seq[Attribute] = Seq(
      AttributeReference("catalog", StringType, nullable = false)(),
      AttributeReference("namespace", ArrayType(StringType, containsNull = false))(),
      AttributeReference("schema", StringType)(),
      AttributeReference("procedure_name", StringType, nullable = false)()
    )) extends UnaryRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    child match {
      case ResolvedNamespace(catalog, ns, _) =>
        val procedureCatalog = catalog.asProcedureCatalog
        val procedures = procedureCatalog.listProcedures(ns.toArray)

        procedures.toSeq.map{ p =>
          val schema = if (p.namespace() != null && p.namespace().nonEmpty) {
            p.namespace().last
          } else {
            null
          }
          Row(catalog.name, p.namespace(), schema, p.name)
        }
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}
