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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalog.v2.{Identifier, TableCatalog}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRowWithSchema}
import org.apache.spark.sql.catalyst.plans.ShowTablesSchema
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}

/**
 * Physical plan node for showing tables.
 */
case class ShowTablesExec(catalog: TableCatalog, ident: Identifier, pattern: Option[String])
    extends LeafExecNode {
  // TODO: "pattern" is not yet supported.
  require(pattern.isEmpty)

  override def output: Seq[AttributeReference] = ShowTablesSchema.attributes()

  override protected def doExecute(): RDD[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()
    val encoder = RowEncoder(ShowTablesSchema.SHOW_TABLES_SCHEMA).resolveAndBind()

    val tables = catalog.listTables(ident.namespace() :+ ident.name())
    tables.map { table =>
      rows += encoder.toRow(
        new GenericRowWithSchema(
          // TODO: there is no v2 catalog API to retrieve 'isTemporary',
          //  and it is set to true for time being.
          Array("", table.name(), true),
          ShowTablesSchema.SHOW_TABLES_SCHEMA))
    }

    sparkContext.parallelize(rows, 1)
  }
}
