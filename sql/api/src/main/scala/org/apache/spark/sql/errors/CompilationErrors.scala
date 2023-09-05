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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.internal.SqlApiConf

private[sql] trait CompilationErrors extends DataTypeErrorsBase {
  def ambiguousColumnOrFieldError(name: Seq[String], numMatches: Int): AnalysisException = {
    new AnalysisException(
      errorClass = "AMBIGUOUS_COLUMN_OR_FIELD",
      messageParameters = Map(
        "name" -> toSQLId(name),
        "n" -> numMatches.toString))
  }

  def columnNotFoundError(colName: String): AnalysisException = {
    new AnalysisException(
      errorClass = "COLUMN_NOT_FOUND",
      messageParameters = Map(
        "colName" -> toSQLId(colName),
        "caseSensitiveConfig" -> toSQLConf(SqlApiConf.CASE_SENSITIVE_KEY)))
  }

  def descriptorParseError(cause: Throwable): AnalysisException = {
    new AnalysisException(
      errorClass = "CANNOT_PARSE_PROTOBUF_DESCRIPTOR",
      messageParameters = Map.empty,
      cause = Option(cause))
  }

  def cannotFindDescriptorFileError(filePath: String, cause: Throwable): AnalysisException = {
    new AnalysisException(
      errorClass = "PROTOBUF_DESCRIPTOR_FILE_NOT_FOUND",
      messageParameters = Map("filePath" -> filePath),
      cause = Option(cause))
  }
}

private[sql] object CompilationErrors extends CompilationErrors
