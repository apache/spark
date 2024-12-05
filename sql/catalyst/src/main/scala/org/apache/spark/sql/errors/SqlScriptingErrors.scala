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

package org.apache.spark.sql.errors

import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.catalyst.util.QuotingUtils.toSQLConf
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryExecutionErrors.toSQLStmt
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf

/**
 * Object for grouping error messages thrown during parsing/interpreting phase
 * of the SQL Scripting Language interpreter.
 */
private[sql] object SqlScriptingErrors {

  def duplicateLabels(origin: Origin, label: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "LABEL_ALREADY_EXISTS",
      cause = null,
      messageParameters = Map("label" -> toSQLId(label)))
  }

  def labelsMismatch(origin: Origin, beginLabel: String, endLabel: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "LABELS_MISMATCH",
      cause = null,
      messageParameters = Map("beginLabel" -> toSQLId(beginLabel), "endLabel" -> toSQLId(endLabel)))
  }

  def endLabelWithoutBeginLabel(origin: Origin, endLabel: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "END_LABEL_WITHOUT_BEGIN_LABEL",
      cause = null,
      messageParameters = Map("endLabel" -> toSQLId(endLabel)))
  }

  def variableDeclarationNotAllowedInScope(
      origin: Origin,
      varName: Seq[String]): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "INVALID_VARIABLE_DECLARATION.NOT_ALLOWED_IN_SCOPE",
      cause = null,
      messageParameters = Map("varName" -> toSQLId(varName)))
  }

  def variableDeclarationOnlyAtBeginning(
      origin: Origin,
      varName: Seq[String]): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "INVALID_VARIABLE_DECLARATION.ONLY_AT_BEGINNING",
      cause = null,
      messageParameters = Map("varName" -> toSQLId(varName)))
  }

  def invalidBooleanStatement(
      origin: Origin,
      stmt: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "INVALID_BOOLEAN_STATEMENT",
      cause = null,
      messageParameters = Map("invalidStatement" -> toSQLStmt(stmt)))
  }

  def sqlScriptingNotEnabled(origin: Origin): Throwable = {
    new SqlScriptingException(
      errorClass = "UNSUPPORTED_FEATURE.SQL_SCRIPTING",
      cause = null,
      origin = origin,
      messageParameters = Map(
        "sqlScriptingEnabled" -> toSQLConf(SQLConf.SQL_SCRIPTING_ENABLED.key)))
  }

  def booleanStatementWithEmptyRow(
      origin: Origin,
      stmt: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "BOOLEAN_STATEMENT_WITH_EMPTY_ROW",
      cause = null,
      messageParameters = Map("invalidStatement" -> toSQLStmt(stmt)))
  }

  def positionalParametersAreNotSupportedWithSqlScripting(): Throwable = {
    new SqlScriptingException(
      origin = null,
      errorClass = "UNSUPPORTED_FEATURE.SQL_SCRIPTING_WITH_POSITIONAL_PARAMETERS",
      cause = null,
      messageParameters = Map.empty)
  }

  def labelDoesNotExist(
      origin: Origin,
      labelName: String,
      statementType: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "INVALID_LABEL_USAGE.DOES_NOT_EXIST",
      cause = null,
      messageParameters = Map(
        "labelName" -> toSQLStmt(labelName),
        "statementType" -> statementType))
  }

  def invalidIterateLabelUsageForCompound(
      origin: Origin,
      labelName: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "INVALID_LABEL_USAGE.ITERATE_IN_COMPOUND",
      cause = null,
      messageParameters = Map("labelName" -> toSQLStmt(labelName)))
  }
}
