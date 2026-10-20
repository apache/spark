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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, RowOrdering}
import org.apache.spark.sql.catalyst.expressions.st.STExpressionUtils.isGeoSpatialType
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalNumericType}
import org.apache.spark.sql.errors.{DataTypeErrors, QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Functions to help with checking for valid data types and value comparison of various types.
 */
object TypeUtils extends QueryErrorsBase {

  def checkForOrderingExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (RowOrdering.isOrderable(dt)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "INVALID_ORDERING_TYPE",
        Map(
          "functionName" -> toSQLId(caller),
          "dataType" -> toSQLType(dt)
        )
      )
    }
  }

  def tryThrowNotOrderableExpression(expression: Expression): Unit = {
    if (!RowOrdering.isOrderable(expression.dataType)) {
      expression.failAnalysis(
        errorClass = "EXPRESSION_TYPE_IS_NOT_ORDERABLE",
        messageParameters =
          Map("expr" -> toSQLExpr(expression), "exprType" -> toSQLType(expression.dataType))
      )
    }
  }

  def checkForSameTypeInputExpr(types: Seq[DataType], caller: String): TypeCheckResult = {
    if (TypeCoercion.haveSameType(types)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "DATA_DIFF_TYPES",
        messageParameters = Map(
          "functionName" -> toSQLId(caller),
          "dataType" -> types.map(toSQLType).mkString("(", " or ", ")")
        )
      )
    }
  }

  def checkForMapKeyType(keyType: DataType): TypeCheckResult = {
    if (keyType.existsRecursively(dt => dt.isInstanceOf[MapType] || dt.isInstanceOf[VariantType] ||
      dt.isInstanceOf[FileType])) {
      DataTypeMismatch(
        errorSubClass = "INVALID_MAP_KEY_TYPE",
        messageParameters = Map(
          "keyType" -> toSQLType(keyType)
        )
      )
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  def checkForAnsiIntervalOrNumericType(input: Expression): TypeCheckResult = input.dataType match {
    case _: AnsiIntervalType | NullType =>
      TypeCheckResult.TypeCheckSuccess
    case dt if dt.isInstanceOf[NumericType] => TypeCheckResult.TypeCheckSuccess
    case other =>
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_INPUT_TYPE",
        messageParameters = Map(
          "paramIndex" -> ordinalNumber(0),
          "requiredType" -> Seq(NumericType, AnsiIntervalType).map(toSQLType).mkString(" or "),
          "inputSql" -> toSQLExpr(input),
          "inputType" -> toSQLType(other)))
  }

  def getNumeric(t: DataType, exactNumericRequired: Boolean = false): Numeric[Any] = {
    if (exactNumericRequired) {
      PhysicalNumericType.exactNumeric(t.asInstanceOf[NumericType])
    } else {
      PhysicalNumericType.numeric(t.asInstanceOf[NumericType])
    }
  }

  @scala.annotation.tailrec
  def getInterpretedOrdering(t: DataType): Ordering[Any] = {
    t match {
      case udt: UserDefinedType[_] => getInterpretedOrdering(udt.sqlType)
      case other => PhysicalDataType.ordering(other)
    }
  }

  /**
   * Returns true if the equals method of the elements of the data type is implemented properly.
   * This also means that they can be safely used in collections relying on the equals method,
   * as sets or maps.
   */
  def typeWithProperEquals(dataType: DataType): Boolean = dataType match {
    case BinaryType => false
    case s: StringType => s.supportsBinaryEquality
    case _: AtomicType => true
    case _ => false
  }

  def failWithIntervalType(dataType: DataType): Unit = {
    invokeOnceForInterval(dataType, forbidAnsiIntervals = false) {
      throw QueryCompilationErrors.cannotUseIntervalTypeInTableSchemaError()
    }
  }

  def invokeOnceForInterval(dataType: DataType, forbidAnsiIntervals: Boolean)(f: => Unit): Unit = {
    def isInterval(dataType: DataType): Boolean = dataType match {
      case _: AnsiIntervalType => forbidAnsiIntervals
      case CalendarIntervalType => true
      case _ => false
    }
    if (dataType.existsRecursively(isInterval)) f
  }

  private def containsTimestampNanosType(dataType: DataType): Boolean = {
    dataType.existsRecursively {
      case _: AnyTimestampNanoType => true
      case _ => false
    }
  }

  def failUnsupportedDataType(dataType: DataType, conf: SQLConf): Unit = {
    if (!conf.isTimeTypeEnabled && dataType.existsRecursively(_.isInstanceOf[TimeType])) {
      throw QueryCompilationErrors.unsupportedTimeTypeError()
    }
    if (!conf.timestampNanosTypesEnabled && containsTimestampNanosType(dataType)) {
      throw DataTypeErrors.timestampNanosTypesNotEnabledError()
    }
    if (!conf.isFileTypeEnabled && dataType.existsRecursively(_.isInstanceOf[FileType])) {
      throw QueryCompilationErrors.unsupportedFileTypeError()
    }
    if (!conf.geospatialEnabled && dataType.existsRecursively(isGeoSpatialType)) {
      throw new org.apache.spark.sql.AnalysisException(
        errorClass = "UNSUPPORTED_FEATURE.GEOSPATIAL_DISABLED",
        messageParameters = scala.collection.immutable.Map.empty)
    }
  }

  private def containsUnknownFileType(dataType: DataType): Boolean = {
    dataType.existsRecursively {
      case ft: FileType => ft.mode == FileTypeMode.UNKNOWN
      case _ => false
    }
  }

  /**
   * Fails if a FILE in `dataType`, at any nesting depth, has no qualifier, as a FILE in a table
   * column definition must be declared `FILE EXTERNAL`.
   */
  def checkFileTypeHasQualifier(dataType: DataType): Unit = {
    if (containsUnknownFileType(dataType)) {
      throw QueryCompilationErrors.fileTypeMissingQualifierError()
    }
  }

  /**
   * Fails if any of `columns`, which a write such as CREATE TABLE AS SELECT stores in a table,
   * holds a FILE without a qualifier, as a table cannot store one.
   */
  def checkNoUnknownFileTypeWrite(columns: Seq[Attribute]): Unit = {
    val unknownCols = columns.filter(c => containsUnknownFileType(c.dataType))
    if (unknownCols.nonEmpty) {
      throw QueryCompilationErrors.fileTypeUnknownWriteError(unknownCols.map(_.name))
    }
  }

  /**
   * Fails if `field` holds a FILE, as FILE has no defined ordering and so cannot be a partition
   * column (`usage` is `PARTITION_COLUMN`) or a clustering column (`CLUSTERING_COLUMN`).
   */
  def checkNoFileTypeInColumnUsage(field: StructField, usage: String): Unit = {
    if (field.dataType.existsRecursively(_.isInstanceOf[FileType])) {
      throw QueryCompilationErrors.fileTypeUnsupportedColumnUsageError(field, usage)
    }
  }

  /**
   * Fails if a FILE in the parameter or return types of a routine has a qualifier. Routines
   * declare a plain FILE; a FILE of any mode can still be passed to them.
   */
  def checkNoQualifierInRoutineSignature(
      dataTypes: Seq[DataType],
      routineType: String,
      routineName: String): Unit = {
    val hasQualified = dataTypes.exists(_.existsRecursively {
      case ft: FileType => ft.mode != FileTypeMode.UNKNOWN
      case _ => false
    })
    if (hasQualified) {
      throw QueryCompilationErrors.fileTypeInRoutineSignatureUnsupportedQualifierError(
        routineType, routineName)
    }
  }

  /**
   * Fails if the return type of a function holds a FILE in a map key. Map construction already
   * rejects such a key through [[checkForMapKeyType]].
   */
  def checkNoFileTypeMapKeyInReturnType(returnType: DataType, routineName: String): Unit = {
    def fileTypeInMapKey(dt: DataType): Boolean = dt match {
      case mt: MapType =>
        mt.keyType.existsRecursively(_.isInstanceOf[FileType]) || fileTypeInMapKey(mt.valueType)
      case at: ArrayType => fileTypeInMapKey(at.elementType)
      case st: StructType => st.fields.exists(f => fileTypeInMapKey(f.dataType))
      case _ => false
    }
    if (fileTypeInMapKey(returnType)) {
      throw QueryCompilationErrors.fileTypeInRoutineSignatureUnsupportedMapKeyError(
        "function", routineName)
    }
  }

  /**
   * Checks the return type of a function that is not created by SQL DDL, for example a UDF
   * registered through `spark.udf.register`, as [[checkNoQualifierInRoutineSignature]] and
   * [[checkNoFileTypeMapKeyInReturnType]] do for SQL functions. Does nothing when the FILE type
   * is disabled.
   */
  def checkNoQualifierInRegisteredRoutineReturnType(
      returnType: DataType,
      routineName: String): Unit = {
    if (SQLConf.get.isFileTypeEnabled) {
      checkNoQualifierInRoutineSignature(Seq(returnType), "function", routineName)
      checkNoFileTypeMapKeyInReturnType(returnType, routineName)
    }
  }
}
