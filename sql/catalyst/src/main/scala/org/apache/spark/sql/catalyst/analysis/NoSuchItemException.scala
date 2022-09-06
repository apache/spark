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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType


/**
 * Thrown by a catalog when an item cannot be found. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
case class NoSuchDatabaseException(db: String)
  extends AnalysisException(errorClass = "SCHEMA_NOT_FOUND",
    messageParameters = Map("schema_name" -> quoteIdentifier(db)))

class NoSuchNamespaceException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(namespace: Seq[String]) = {
    this(errorClass = "SCHEMA_NOT_FOUND",
      Map("schema_name" -> namespace.map(part => quoteIdentifier(part)).mkString(".")))
  }
  def this(namespace: Array[String]) = {
    this(errorClass = "SCHEMA_NOT_FOUND",
      Map("schema_name" -> namespace.map(part => quoteIdentifier(part)).mkString(".")))
  }
}

class NoSuchTableException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(db: String, table: String) = {
    this(errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relation_name" ->
        (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(name : Seq[String]) = {
    this(errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relation_name" ->
        name.map(part => quoteIdentifier(part)).mkString(".")))
  }

  def this(tableIdent: Identifier) = {
    this(errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relation_name" -> tableIdent.quoted))
  }

  def this(table: String) = {
    this(errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relation_name" ->
        UnresolvedAttribute.parseAttributeName(table)
        .map(part => quoteIdentifier(part)).mkString(".")))
  }
}

class NoSuchPartitionException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(db: String, table: String, spec: TablePartitionSpec) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partition_list" ->
        ("PARTITION (" +
          spec.map( kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", ") + ")"),
        "table_name" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(tableName: String, partitionIdent: InternalRow, partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partition_list" ->
        ("PARTITION (" + partitionIdent.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
        .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}").mkString(", ") + ")"),
        "table_name" -> UnresolvedAttribute.parseAttributeName(tableName)
          .map(part => quoteIdentifier(part)).mkString(".")))
  }
}

class NoSuchPermanentFunctionException(db: String, func: String)
  extends AnalysisException(errorClass = "ROUTINE_NOT_FOUND",
    Map("routine_name" -> (quoteIdentifier(db) + "." + quoteIdentifier(func))))

class NoSuchFunctionException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(db: String, func: String) = {
    this(errorClass = "ROUTINE_NOT_FOUND",
      Map("routine_name" -> (quoteIdentifier(db) + "." + quoteIdentifier(func))))
  }

  def this(identifier: Identifier) = {
    this(errorClass = "ROUTINE_NOT_FOUND", Map("routine_name" -> identifier.quoted))
  }
}

class NoSuchPartitionsException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(db: String, table: String, specs: Seq[TablePartitionSpec]) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partition_list" -> ("PARTITION (" +
        specs.map(spec => spec.map(kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", "))
        .mkString("), PARTITION (") + ")"),
        "table_name" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(tableName: String, partitionIdents: Seq[InternalRow], partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partition_list" -> ("PARTITION (" +
        partitionIdents.map(_.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
          .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}")
          .mkString(", ")).mkString("), PARTITION (") + ")"),
        "table_name" -> UnresolvedAttribute.parseAttributeName(tableName)
          .map(part => quoteIdentifier(part)).mkString(".")))
  }
}

class NoSuchTempFunctionException(func: String)
  extends AnalysisException(errorClass = "ROUTINE_NOT_FOUND", Map("routine_name" -> s"`$func`"))

class NoSuchIndexException(message: String, cause: Option[Throwable] = None)
  extends AnalysisException(errorClass = "INDEX_NOT_FOUND",
    Map("message" -> message), cause)
