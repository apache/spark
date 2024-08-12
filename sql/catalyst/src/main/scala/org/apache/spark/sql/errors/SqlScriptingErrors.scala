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
import org.apache.spark.sql.errors.QueryExecutionErrors.toSQLStmt
import org.apache.spark.sql.exceptions.SqlScriptingException
import org.apache.spark.sql.internal.SQLConf

/**
 * Object for grouping error messages thrown during parsing/interpreting phase
 * of the SQL Scripting Language interpreter.
 */
private[sql] object SqlScriptingErrors {

  def labelsMismatch(origin: Origin, beginLabel: String, endLabel: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "LABELS_MISMATCH",
      cause = null,
      messageParameters = Map("beginLabel" -> beginLabel, "endLabel" -> endLabel))
  }

  def endLabelWithoutBeginLabel(origin: Origin, endLabel: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "END_LABEL_WITHOUT_BEGIN_LABEL",
      cause = null,
      messageParameters = Map("endLabel" -> endLabel))
  }

  def sqlScriptingNotEnabled(origin: Origin): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "UNSUPPORTED_FEATURE.SQL_SCRIPTING_NOT_ENABLED",
      cause = null,
      messageParameters = Map("sqlScriptingEnabled" -> SQLConf.SQL_SCRIPTING_ENABLED.key))
  }

  def duplicateHandlerForSameSqlState(origin: Origin, sqlState: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "DUPLICATE_HANDLER_FOR_SAME_SQL_STATE",
      cause = null,
      messageParameters = Map("sqlState" -> sqlState))
  }

  def duplicateSqlStateForSameHandler(origin: Origin, sqlState: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
          errorClass = "DUPLICATE_SQL_STATE_FOR_SAME_HANDLER",
      cause = null,
      messageParameters = Map("sqlState" -> sqlState))
  }

  def duplicateConditionNameForDifferentSqlState(
      origin: Origin,
      conditionName: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "DUPLICATE_CONDITION_NAME_FOR_DIFFERENT_SQL_STATE",
      cause = null,
      messageParameters = Map("conditionName" -> conditionName))
  }

  def variableDeclarationNotAllowedInScope(
      origin: Origin,
      varName: String,
      lineNumber: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "INVALID_VARIABLE_DECLARATION.NOT_ALLOWED_IN_SCOPE",
      cause = null,
      messageParameters = Map("varName" -> varName, "lineNumber" -> lineNumber))
  }

  def variableDeclarationOnlyAtBeginning(
      origin: Origin,
      varName: String,
      lineNumber: String): Throwable = {
    new SqlScriptingException(
      origin = origin,
      errorClass = "INVALID_VARIABLE_DECLARATION.ONLY_AT_BEGINNING",
      cause = null,
      messageParameters = Map("varName" -> varName, "lineNumber" -> lineNumber))
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
}
