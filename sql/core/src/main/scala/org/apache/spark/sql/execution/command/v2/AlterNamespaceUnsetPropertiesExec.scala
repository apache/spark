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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{NamespaceChange, SupportsNamespaces}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

/**
 * Physical plan node for unsetting properties of namespace.
 */
case class AlterNamespaceUnsetPropertiesExec(
    catalog: SupportsNamespaces,
    namespace1: Seq[String],
    namespace: ResolvedNamespace,
    propKeys: Seq[String],
    ifExists: Boolean) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    if (!ifExists) {
      val properties = namespace.metadata
      val nonexistentKeys = propKeys.filter(key => !properties.contains(key))
      if (nonexistentKeys.nonEmpty) {
        throw QueryCompilationErrors.unsetNonExistentPropertiesError(
          nonexistentKeys, namespace.namespace)
      }
    }
    val changes = propKeys.map {
      NamespaceChange.removeProperty
    }
    // catalog.alterNamespace(namespace.toArray, changes: _*)
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
