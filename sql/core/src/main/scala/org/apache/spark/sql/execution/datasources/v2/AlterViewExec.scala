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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, ViewCatalog, ViewChange}

/**
 * Physical plan node for altering a view.
 */
case class AlterViewExec(
    catalog: ViewCatalog,
    ident: Identifier,
    changes: Seq[ViewChange]) extends LeafV2CommandExec {

  override protected def run(): Seq[InternalRow] = {
    try {
      catalog.alterView(ident, changes: _*)
    } catch {
      case e: IllegalArgumentException =>
        throw new SparkException(s"Invalid view change: ${e.getMessage}", e)
      case e: UnsupportedOperationException =>
        throw new SparkException(s"Unsupported view change: ${e.getMessage}", e)
    }

    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
