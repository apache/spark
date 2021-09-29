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

import java.sql.SQLFeatureNotSupportedException

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.IndexAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.connector.expressions.FieldReference

/**
 * Physical plan node for creating an index.
 */
case class CreateIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: String,
    indexType: String,
    ifNotExists: Boolean,
    columns: Seq[FieldReference],
    columnProperties: Seq[Map[String, String]],
    private var properties: Map[String, String])
  extends LeafV2CommandExec {
  override protected def run(): Seq[InternalRow] = {

    try {
      // Todo: replace this with the createIndex
      throw new SQLFeatureNotSupportedException(s"CreateIndex not supported yet." +
        s" IndexName $indexName indexType $indexType columns $columns" +
        s" columnProperties $columnProperties properties $properties")
    } catch {
      case _: IndexAlreadyExistsException if ifNotExists =>
        logWarning(s"Index ${indexName} already exists. Ignoring.")
    }
    Seq.empty
  }

  override def output: Seq[Attribute] = Seq.empty
}
