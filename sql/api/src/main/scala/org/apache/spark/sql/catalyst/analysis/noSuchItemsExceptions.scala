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
import org.apache.spark.sql.catalyst.util.QuotingUtils.{quoted, quoteIdentifier, quoteNameParts}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.util.ArrayImplicits._

/**
 * Thrown by a catalog when an item cannot be found. The analyzer will rethrow the exception
 * as an [[org.apache.spark.sql.AnalysisException]] with the correct position information.
 */
class NoSuchDatabaseException private[analysis](
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
      cause = cause,
      Some(errorClass),
      messageParameters)
  }

  def this(db: String) = {
    this(
      errorClass = "SCHEMA_NOT_FOUND",
      messageParameters = Map("schemaName" -> quoteIdentifier(db)),
      cause = None)
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchNamespaceException private(
    message: String,
    cause: Option[Throwable],
    errorClass: Option[String],
    messageParameters: Map[String, String])
  extends NoSuchDatabaseException(
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
      Map("schemaName" -> quoteNameParts(namespace.toImmutableArraySeq)))
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

  def this(errorClass: String, messageParameters: Map[String, String], cause: Option[Throwable]) = {
    this(
      SparkThrowableHelper.getMessage(errorClass, messageParameters),
      cause = cause,
      Some(errorClass),
      messageParameters)
  }

  def this(db: String, table: String) = {
    this(
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" ->
        (quoteIdentifier(db) + "." + quoteIdentifier(table))),
      cause = None)
  }

  def this(name : Seq[String]) = {
    this(
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> quoteNameParts(name)),
      cause = None)
  }

  def this(tableIdent: Identifier) = {
    this(
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> quoted(tableIdent)),
      cause = None)
  }
}

// any changes to this class should be backward compatible as it may be used by external connectors
class NoSuchViewException(errorClass: String, messageParameters: Map[String, String])
  extends AnalysisException(errorClass, messageParameters) {

  def this(ident: Identifier) =
    this(errorClass = "VIEW_NOT_FOUND",
      messageParameters = Map("relationName" -> quoted(ident)))
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
    this(errorClass = "ROUTINE_NOT_FOUND", Map("routineName" -> quoted(identifier)))
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
}
