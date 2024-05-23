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

import org.apache.spark.internal.LogKeys.INDEX_NAME
import org.apache.spark.internal.MDC
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchIndexException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.index.SupportsIndex

/**
 * Physical plan node for dropping an index.
 */
case class DropIndexExec(
    table: SupportsIndex,
    indexName: String,
    ignoreIfNotExists: Boolean) extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {
    try {
      table.dropIndex(indexName)
    } catch {
      case _: NoSuchIndexException if ignoreIfNotExists =>
        logWarning(log"Index ${MDC(INDEX_NAME, indexName)} does not exist. Ignoring.")
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
