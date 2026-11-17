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

package org.apache.spark.graph

import org.apache.spark.{SparkException, SparkIllegalArgumentException, SparkRuntimeException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.util.QuotingUtils.quoteIdentifier
import org.apache.spark.sql.types.DataType

private[graph] object GraphErrors {

  val VERTICES: String = "vertices"
  val EDGES: String = "edges"
  val VERTEX_PROPERTIES: String = "vertex properties"
  val EDGE_PROPERTIES: String = "edge properties"

  def cannotInvokeInTransformationsError(): SparkException = {
    new SparkException(
      errorClass = "GRAPH_CANNOT_INVOKE_IN_TRANSFORMATIONS",
      messageParameters = Map.empty,
      cause = null)
  }

  def nullDataFrameError(dataFrameKind: String): SparkIllegalArgumentException = {
    new SparkIllegalArgumentException(
      errorClass = "GRAPH_NULL_DATAFRAME",
      messageParameters = Map("dataFrameKind" -> dataFrameKind))
  }

  def missingRequiredColumnError(
      dataFrameKind: String,
      columnName: String,
      columns: Seq[String]): AnalysisException = {
    new AnalysisException(
      errorClass = "GRAPH_MISSING_REQUIRED_COLUMN",
      messageParameters = Map(
        "dataFrameKind" -> dataFrameKind,
        "columnName" -> quoteIdentifier(columnName),
        "availableColumns" -> columns.map(quoteIdentifier).mkString(", ")))
  }

  def ambiguousRequiredColumnError(
      dataFrameKind: String,
      columnName: String,
      matchingColumns: Seq[String]): AnalysisException = {
    new AnalysisException(
      errorClass = "GRAPH_AMBIGUOUS_REQUIRED_COLUMN",
      messageParameters = Map(
        "dataFrameKind" -> dataFrameKind,
        "count" -> matchingColumns.length.toString,
        "columnName" -> quoteIdentifier(columnName),
        "matchingColumns" -> matchingColumns.map(quoteIdentifier).mkString(", ")))
  }

  def unsupportedIdTypeError(
      dataFrameKind: String,
      columnName: String,
      columnType: DataType,
      supportedTypes: Seq[DataType]): AnalysisException = {
    new AnalysisException(
      errorClass = "GRAPH_UNSUPPORTED_ID_TYPE",
      messageParameters = Map(
        "dataFrameKind" -> dataFrameKind,
        "columnName" -> quoteIdentifier(columnName),
        "columnType" -> toSQLType(columnType),
        "supportedTypes" -> supportedTypes.map(toSQLType).mkString(", ")))
  }

  def idTypeMismatchError(
      idColumn: String,
      idType: DataType,
      srcColumn: String,
      srcType: DataType,
      dstColumn: String,
      dstType: DataType): AnalysisException = {
    new AnalysisException(
      errorClass = "GRAPH_ID_TYPE_MISMATCH",
      messageParameters = Map(
        "idColumn" -> quoteIdentifier(idColumn),
        "srcColumn" -> quoteIdentifier(srcColumn),
        "dstColumn" -> quoteIdentifier(dstColumn),
        "idType" -> toSQLType(idType),
        "srcType" -> toSQLType(srcType),
        "dstType" -> toSQLType(dstType)))
  }

  def propertiesKeyTypeMismatchError(
      dataFrameKind: String,
      columnName: String,
      actualType: DataType,
      expectedType: DataType): AnalysisException = {
    new AnalysisException(
      errorClass = "GRAPH_PROPERTIES_KEY_TYPE_MISMATCH",
      messageParameters = Map(
        "dataFrameKind" -> dataFrameKind,
        "columnName" -> quoteIdentifier(columnName),
        "actualType" -> toSQLType(actualType),
        "expectedType" -> toSQLType(expectedType)))
  }

  def propertiesColumnConflictError(
      dataFrameKind: String,
      targetKind: String,
      conflictingColumns: Seq[String]): AnalysisException = {
    new AnalysisException(
      errorClass = "GRAPH_PROPERTIES_COLUMN_CONFLICT",
      messageParameters = Map(
        "dataFrameKind" -> dataFrameKind,
        "targetKind" -> targetKind,
        "conflictingColumns" -> conflictingColumns.map(quoteIdentifier).mkString(", ")))
  }

  private def toSQLType(dataType: DataType): String = "\"" + dataType.sql + "\""

  def nullIdsError(dataFrameKind: String, columns: Seq[String]): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "GRAPH_INVALID.NULL_IDS",
      messageParameters = Map(
        "dataFrameKind" -> dataFrameKind,
        "columns" -> columns.map(quoteIdentifier).mkString(", ")))
  }

  def duplicateVerticesError(count: Long, idColumn: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "GRAPH_INVALID.DUPLICATE_VERTICES",
      messageParameters = Map(
        "count" -> count.toString,
        "idColumn" -> quoteIdentifier(idColumn)))
  }

  def edgeVerticesCountMismatchError(
      edgeVertexCount: Long,
      vertexCount: Long): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "GRAPH_INVALID.EDGE_VERTICES_COUNT_MISMATCH",
      messageParameters = Map(
        "edgeVertexCount" -> edgeVertexCount.toString,
        "vertexCount" -> vertexCount.toString))
  }

  def edgeVertexNotInVerticesError(
      count: Long,
      srcColumn: String,
      dstColumn: String): SparkRuntimeException = {
    new SparkRuntimeException(
      errorClass = "GRAPH_INVALID.EDGE_VERTEX_NOT_IN_VERTICES",
      messageParameters = Map(
        "count" -> count.toString,
        "srcColumn" -> quoteIdentifier(srcColumn),
        "dstColumn" -> quoteIdentifier(dstColumn)))
  }
}
