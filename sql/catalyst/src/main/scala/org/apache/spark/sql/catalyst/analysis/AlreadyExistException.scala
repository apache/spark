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
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, quoteNameParts }
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType

/**
 * Thrown by a catalog when an item already exists. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
class DatabaseAlreadyExistsException(db: String)
  extends NamespaceAlreadyExistsException(Array(db))


class NamespaceAlreadyExistsException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {
  def this(namespace: Array[String]) = {
    this(errorClass = "SCHEMA_ALREADY_EXISTS",
      Map("schemaName" -> quoteNameParts(namespace)))
  }
}


class TableAlreadyExistsException(errorClass: String, messageParameters: Map[String, String],
  cause: Option[Throwable] = None)
  extends AnalysisException(errorClass, messageParameters, cause = cause) {
  def this(db: String, table: String) = {
    this(errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" ->
        (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(table: String) = {
    this(errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" ->
        quoteNameParts(UnresolvedAttribute.parseAttributeName(table))))
  }

  def this(table: Seq[String]) = {
    this(errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" -> quoteNameParts(table)))
  }
}

class TempTableAlreadyExistsException(errorClass: String, messageParameters: Map[String, String],
                                      cause: Option[Throwable] = None)
  extends AnalysisException(errorClass, messageParameters, cause = cause) {
  def this(table: String) = {
    this(errorClass = "TEMP_TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName"
        -> quoteNameParts(UnresolvedAttribute.parseAttributeName(table))))
  }
}

class ViewAlreadyExistsException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(ident: Identifier) =
    this(errorClass = "VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" -> ident.quoted))
}

class PartitionAlreadyExistsException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {
  def this(db: String, table: String, spec: TablePartitionSpec) = {
    this(errorClass = "PARTITIONS_ALREADY_EXIST",
      Map("partitionList" -> ("PARTITION (" +
        spec.map( kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", ") + ")"),
        "tableName" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(tableName: String, partitionIdent: InternalRow, partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_ALREADY_EXIST",
      Map("partitionList" ->
        ("PARTITION (" + partitionIdent.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
        .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}").mkString(", ") + ")"),
        "tableName" -> quoteNameParts(UnresolvedAttribute.parseAttributeName(tableName))))
  }
}

class PartitionsAlreadyExistException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {
  def this(db: String, table: String, specs: Seq[TablePartitionSpec]) = {
    this(errorClass = "PARTITIONS_ALREADY_EXIST",
      Map("partitionList" ->
        ("PARTITION ("
        + specs.map(spec => spec.map(kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", "))
        .mkString("), PARTITION (") + ")"),
        "tableName" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(db: String, table: String, spec: TablePartitionSpec) =
    this(db, table, Seq(spec))

  def this(tableName: String, partitionIdents: Seq[InternalRow], partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_ALREADY_EXIST",
      Map("partitionList" ->
        ("PARTITION (" +
          partitionIdents.map(_.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
            .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}")
            .mkString(", ")).mkString("), PARTITION (") + ")"),
        "tableName" -> quoteNameParts(UnresolvedAttribute.parseAttributeName(tableName))))
  }

  def this(tableName: String, partitionIdent: InternalRow, partitionSchema: StructType) =
    this(tableName, Seq(partitionIdent), partitionSchema)
}

class FunctionAlreadyExistsException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(function: Seq[String]) = {
    this (errorClass = "ROUTINE_ALREADY_EXISTS",
      Map("routineName" -> quoteNameParts(function)))
  }
}

class IndexAlreadyExistsException(message: String, cause: Option[Throwable] = None)
  extends AnalysisException(errorClass = "INDEX_ALREADY_EXISTS",
    Map("message" -> message), cause)
