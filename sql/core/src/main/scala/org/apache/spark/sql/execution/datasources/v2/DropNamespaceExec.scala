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
import org.apache.spark.sql.connector.catalog.SupportsNamespaces

/**
 * Physical plan node for dropping a namespace.
 */
case class DropNamespaceExec(
    catalog: SupportsNamespaces,
    namespace: Seq[String],
    ifExists: Boolean,
    cascade: Boolean)
  extends V2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    val ns = namespace.toArray
    if (catalog.namespaceExists(ns)) {
      try {
        catalog.dropNamespace(ns)
      } catch {
        case e: IllegalStateException if cascade =>
          throw new SparkException(
            "Cascade option for droping namespace is not supported in V2 catalog", e)
      }
    } else if (!ifExists) {
      throw new NoSuchNamespaceException(ns)
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
