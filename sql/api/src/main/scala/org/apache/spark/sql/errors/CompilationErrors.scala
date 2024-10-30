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
      messageParameters = Map("name" -> toSQLId(name), "n" -> numMatches.toString))
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

  def usingUntypedScalaUDFError(): Throwable = {
    new AnalysisException(errorClass = "UNTYPED_SCALA_UDF", messageParameters = Map.empty)
  }

  def invalidBoundaryStartError(start: Long): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_BOUNDARY.START",
      messageParameters = Map(
        "boundary" -> toSQLId("start"),
        "invalidValue" -> toSQLValue(start),
        "longMinValue" -> toSQLValue(Long.MinValue),
        "intMinValue" -> toSQLValue(Int.MinValue),
        "intMaxValue" -> toSQLValue(Int.MaxValue)))
  }

  def invalidBoundaryEndError(end: Long): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_BOUNDARY.END",
      messageParameters = Map(
        "boundary" -> toSQLId("end"),
        "invalidValue" -> toSQLValue(end),
        "longMaxValue" -> toSQLValue(Long.MaxValue),
        "intMinValue" -> toSQLValue(Int.MinValue),
        "intMaxValue" -> toSQLValue(Int.MaxValue)))
  }

  def invalidSaveModeError(saveMode: String): Throwable = {
    new AnalysisException(
      errorClass = "INVALID_SAVE_MODE",
      messageParameters = Map("mode" -> toDSOption(saveMode)))
  }

  def sortByWithoutBucketingError(): Throwable = {
    new AnalysisException(errorClass = "SORT_BY_WITHOUT_BUCKETING", messageParameters = Map.empty)
  }

  def bucketByUnsupportedByOperationError(operation: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1312",
      messageParameters = Map("operation" -> operation))
  }

  def bucketByAndSortByUnsupportedByOperationError(operation: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1313",
      messageParameters = Map("operation" -> operation))
  }

  def operationNotSupportPartitioningError(operation: String): Throwable = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_1197",
      messageParameters = Map("operation" -> operation))
  }

  def operationNotSupportClusteringError(operation: String): Throwable = {
    new AnalysisException(
      errorClass = "CLUSTERING_NOT_SUPPORTED",
      messageParameters = Map("operation" -> operation))
  }

  def clusterByWithPartitionedBy(): Throwable = {
    new AnalysisException(
      errorClass = "SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED",
      messageParameters = Map.empty)
  }

  def clusterByWithBucketing(): Throwable = {
    new AnalysisException(
      errorClass = "SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED",
      messageParameters = Map.empty)
  }
}

private[sql] object CompilationErrors extends CompilationErrors
