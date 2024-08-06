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
import org.apache.spark.sql.catalyst.util.AttributeNameParser
import org.apache.spark.sql.catalyst.util.QuotingUtils.{quoted, quoteIdentifier, quoteNameParts}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.util.ArrayImplicits._

/**
 * Thrown by a catalog when an item already exists. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
class DatabaseAlreadyExistsException(db: String)
  extends NamespaceAlreadyExistsException(Array(db))

// any changes to this class should be backward compatible as it may be used by external connectors
class NamespaceAlreadyExistsException private(
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

  def this(namespace: Array[String]) = {
    this(errorClass = "SCHEMA_ALREADY_EXISTS",
      Map("schemaName" -> quoteNameParts(namespace.toImmutableArraySeq)))
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class TableAlreadyExistsException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends AnalysisException(
    message,
    cause = cause,
    errorClass = errorClass,
    messageParameters = messageParameters) {

  def this(errorClass: String, messageParameters: Map[String, String], cause: Option[Throwable]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause,
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, table: String) = {
    this(errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" ->
        (quoteIdentifier(db) + "." + quoteIdentifier(table))),
      cause = None)
  }

  def this(table: String) = {
    this(errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" ->
        quoteNameParts(AttributeNameParser.parseAttributeName(table))),
      cause = None)
  }

  def this(table: Seq[String]) = {
    this(errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" -> quoteNameParts(table)),
      cause = None)
  }

  def this(tableIdent: Identifier) = {
    this(errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" -> quoted(tableIdent)),
      cause = None)
  }
}

class TempTableAlreadyExistsException private(
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
    cause: Option[Throwable] = None) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause,
      Some(errorClass),
      messageParameters)
  }

  def this(table: String) = {
    this(
      errorClass = "TEMP_TABLE_OR_VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName"
        -> quoteNameParts(AttributeNameParser.parseAttributeName(table))))
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class ViewAlreadyExistsException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(ident: Identifier) =
    this(errorClass = "VIEW_ALREADY_EXISTS",
      messageParameters = Map("relationName" -> quoted(ident)))
}

// any changes to this class should be backward compatible as it may be used by external connectors
class FunctionAlreadyExistsException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(function: Seq[String]) = {
    this (errorClass = "ROUTINE_ALREADY_EXISTS",
      Map("routineName" -> quoteNameParts(function),
        "newRoutineType" -> "routine",
        "existingRoutineType" -> "routine"))
  }

  def this(db: String, func: String) = {
    this(Seq(db, func))
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class IndexAlreadyExistsException private(
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
    this("INDEX_ALREADY_EXISTS", Map("indexName" -> indexName, "tableName" -> tableName), cause)
  }
}
