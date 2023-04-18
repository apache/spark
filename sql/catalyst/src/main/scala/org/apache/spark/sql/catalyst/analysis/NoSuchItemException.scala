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

import org.apache.spark.SparkThrowableHelper
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.util.{quoteIdentifier, quoteNameParts}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.types.StructType


/**
 * Thrown by a catalog when an item cannot be found. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
case class NoSuchDatabaseException(db: String)
  extends AnalysisException(errorClass = "SCHEMA_NOT_FOUND",
    messageParameters = Map("schemaName" -> quoteIdentifier(db)))

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchNamespaceException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    cause = cause,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause = None,
      Some(errorClass),
      messageParameters)
  }

  def this(namespace: Seq[String]) = {
    this(errorClass = "SCHEMA_NOT_FOUND",
      Map("schemaName" -> quoteNameParts(namespace)))
  }

  def this(namespace: Array[String]) = {
    this(errorClass = "SCHEMA_NOT_FOUND",
      Map("schemaName" -> quoteNameParts(namespace)))
  }

  def this(message: String, cause: Option[Throwable] = None) = {
    this(message, cause, errorClass = None, messageParameters = Map.empty[String, String])
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchTableException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    cause = cause,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause = None,
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, table: String) = {
    this(errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" ->
        (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(name : Seq[String]) = {
    this(errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> quoteNameParts(name)))
  }

  def this(tableIdent: Identifier) = {
    this(errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> tableIdent.quoted))
  }

  def this(message: String, cause: Option[Throwable] = None) = {
    this(message, cause, errorClass = None, messageParameters = Map.empty[String, String])
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchViewException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(ident: Identifier) =
    this(errorClass = "VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> ident.quoted))
}

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchPartitionException private(
    message: String,
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, table: String, spec: TablePartitionSpec) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" ->
        ("PARTITION (" +
          spec.map( kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", ") + ")"),
        "tableName" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(tableName: String, partitionIdent: InternalRow, partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" ->
        ("PARTITION (" + partitionIdent.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
        .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}").mkString(", ") + ")"),
        "tableName" -> quoteNameParts(UnresolvedAttribute.parseAttributeName(tableName))))
  }

  def this(message: String) = {
    this(message, errorClass = None, messageParameters = Map.empty[String, String])
  }
}

class NoSuchPermanentFunctionException(db: String, func: String)
  extends AnalysisException(errorClass = "ROUTINE_NOT_FOUND",
    Map("routineName" -> (quoteIdentifier(db) + "." + quoteIdentifier(func))))

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchFunctionException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    cause = cause,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause = None,
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, func: String) = {
    this(errorClass = "ROUTINE_NOT_FOUND",
      Map("routineName" -> (quoteIdentifier(db) + "." + quoteIdentifier(func))))
  }

  def this(identifier: Identifier) = {
    this(errorClass = "ROUTINE_NOT_FOUND", Map("routineName" -> identifier.quoted))
  }

  def this(message: String, cause: Option[Throwable] = None) = {
    this(message, cause, errorClass = None, messageParameters = Map.empty[String, String])
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchPartitionsException private(
    message: String,
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, table: String, specs: Seq[TablePartitionSpec]) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" -> ("PARTITION (" +
        specs.map(spec => spec.map(kv => quoteIdentifier(kv._1) + s" = ${kv._2}").mkString(", "))
        .mkString("), PARTITION (") + ")"),
        "tableName" -> (quoteIdentifier(db) + "." + quoteIdentifier(table))))
  }

  def this(tableName: String, partitionIdents: Seq[InternalRow], partitionSchema: StructType) = {
    this(errorClass = "PARTITIONS_NOT_FOUND",
      Map("partitionList" -> ("PARTITION (" +
        partitionIdents.map(_.toSeq(partitionSchema).zip(partitionSchema.map(_.name))
          .map( kv => quoteIdentifier(s"${kv._2}") + s" = ${kv._1}")
          .mkString(", ")).mkString("), PARTITION (") + ")"),
        "tableName" -> quoteNameParts(UnresolvedAttribute.parseAttributeName(tableName))))
  }

  def this(message: String) = {
    this(message, errorClass = None, messageParameters = Map.empty[String, String])
  }
}

class NoSuchTempFunctionException(func: String)
  extends AnalysisException(errorClass = "ROUTINE_NOT_FOUND", Map("routineName" -> s"`$func`"))

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchIndexException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    cause = cause,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(
      errorClass: String,
      messageParameters: Map[String, String],
      cause: Option[Throwable]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause,
      Some(errorClass),
      messageParameters)
  }

  def this(indexName: String, tableName: String, cause: Option[Throwable]) = {
    this("INDEX_NOT_FOUND", Map("indexName" -> indexName, "tableName" -> tableName), cause)
  }

  def this(message: String, cause: Option[Throwable] = None) = {
    this(message, cause, errorClass = None, messageParameters = Map.empty[String, String])
  }
}
