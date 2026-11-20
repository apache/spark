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

import org.apache.spark.sql.catalyst.analysis.{AnalysisErrorAt, Resolver, TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.catalog.ClusterBySpec
import org.apache.spark.sql.catalyst.expressions.{Expression, RowOrdering}
import org.apache.spark.sql.catalyst.expressions.st.STExpressionUtils.isGeoSpatialType
import org.apache.spark.sql.catalyst.types.{PhysicalDataType, PhysicalNumericType}
import org.apache.spark.sql.errors.{DataTypeErrors, QueryCompilationErrors, QueryErrorsBase}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Functions to help with checking for valid data types and value comparison of various types.
 */
object TypeUtils extends QueryErrorsBase {

  /**
   * Returns true if the type has a decimal of a negative scale in it
   * (`spark.sql.legacy.allowNegativeScaleOfDecimal`).
   */
  def hasNegativeScale(dt: DataType): Boolean = dt.existsRecursively {
    case d: DecimalType => d.scale < 0
    case _ => false
  }

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

  /**
   * Returns the paths of the columns that hold a FILE without a qualifier, for a column at `path`
   * of type `dataType`. The path descends into struct fields, so a FILE in a field of a struct
   * column is reported at that field. A FILE in an array or a map is reported at the column of
   * the array or map. A schema is given as a struct with an empty path.
   */
  private def unknownFileTypePaths(
      path: Seq[String],
      dataType: DataType): Seq[(Seq[String], DataType)] = {
    val hasUnknownFile = dataType.existsRecursively {
      case ft: FileType => ft.mode == FileTypeMode.UNKNOWN
      case _ => false
    }
    dataType match {
      // Most types hold no FILE, so this is decided before descending into structs.
      case _ if !hasUnknownFile => Seq.empty
      case st: StructType =>
        st.fields.toSeq.flatMap(f => unknownFileTypePaths(path :+ f.name, f.dataType))
      case dt => Seq(path -> dt)
    }
  }

  /**
   * Fails if a FILE in any of `columns`, given as the path of a column and its type, has no
   * qualifier, as a FILE in a table column definition must be declared `FILE EXTERNAL`.
   */
  def checkFileTypeHasQualifier(columns: Seq[(Seq[String], DataType)]): Unit = {
    columns.iterator.flatMap { case (path, dataType) => unknownFileTypePaths(path, dataType) }
      .take(1).foreach { case (path, dataType) =>
        throw QueryCompilationErrors.fileTypeMissingQualifierError(path, dataType)
      }
  }

  /** Fails if a FILE in a column of `schema` has no qualifier. */
  def checkFileTypeHasQualifier(schema: StructType): Unit = {
    checkFileTypeHasQualifier(Seq(Seq.empty[String] -> schema))
  }

  /**
   * Fails if any of `columns`, which a write such as CREATE TABLE AS SELECT, CREATE TABLE LIKE or
   * schema evolution stores in a table, holds a FILE without a qualifier, as a table cannot store
   * one. A column is given as its path and the type it would be stored with.
   */
  def checkNoUnknownFileTypeWrite(columns: Seq[(Seq[String], DataType)]): Unit = {
    val unknownPaths = columns.flatMap { case (path, dataType) =>
      unknownFileTypePaths(path, dataType).map(_._1)
    }
    if (unknownPaths.nonEmpty) {
      throw QueryCompilationErrors.fileTypeUnknownWriteError(unknownPaths)
    }
  }

  /** Fails if a column of `schema`, which a write stores in a table, holds an unknown FILE. */
  def checkNoUnknownFileTypeWrite(schema: StructType): Unit = {
    checkNoUnknownFileTypeWrite(Seq(Seq.empty[String] -> schema))
  }

  /**
   * Fails if the column at `path`, of type `dataType`, holds a FILE, as FILE is not comparable
   * and so cannot have the given `usage`, such as a partition or clustering column.
   */
  def checkNoFileTypeInColumnUsage(
      path: Seq[String],
      dataType: DataType,
      usage: FileTypeColumnUsage): Unit = {
    if (dataType.existsRecursively(_.isInstanceOf[FileType])) {
      throw QueryCompilationErrors.fileTypeUnsupportedColumnUsageError(path, dataType, usage)
    }
  }

  /**
   * Fails if a clustering column of `clusterBySpec` holds a FILE. Only a schema that holds a FILE
   * is checked. A column is looked up as the clustering normalizer does
   * (`SchemaUtils.findColumnPosition`): the first match, descending into structs and the struct
   * elements of arrays. A column that is not found is left to the normalizer, which reports it.
   */
  def checkNoFileTypeClusteringColumn(
      schema: StructType,
      clusterBySpec: ClusterBySpec,
      resolver: Resolver): Unit = {
    // Returns the path of the column, with the names of the schema, and its field.
    def find(schema: StructType, path: Seq[String]): Option[(Seq[String], StructField)] =
      schema.fields.find(f => resolver(f.name, path.head)).flatMap { field =>
        (path.tail, field.dataType) match {
          case (Seq(), _) => Some(Seq(field.name) -> field)
          case (tail, s: StructType) => find(s, tail).map { case (p, f) => (field.name +: p, f) }
          case (tail, ArrayType(s: StructType, _)) =>
            find(s, tail).map { case (p, f) => (field.name +: p, f) }
          case _ => None
        }
      }
    if (schema.existsRecursively(_.isInstanceOf[FileType])) {
      clusterBySpec.columnNames.map(_.fieldNames.toImmutableArraySeq).filter(_.nonEmpty)
        .flatMap(find(schema, _))
        .foreach { case (path, field) =>
          checkNoFileTypeInColumnUsage(path, field.dataType, FileTypeColumnUsage.ClusteringColumn)
        }
    }
  }

  /**
   * Fails if a FILE in the parameter or return types of a routine has a qualifier. Routines
   * declare a plain FILE. `routineName` is quoted already.
   */
  def checkNoQualifierInRoutineSignature(dataTypes: Seq[DataType], routineName: String): Unit = {
    val hasQualified = dataTypes.exists(_.existsRecursively {
      case ft: FileType => ft.mode != FileTypeMode.UNKNOWN
      case _ => false
    })
    if (hasQualified) {
      throw QueryCompilationErrors.fileTypeInRoutineSignatureUnsupportedQualifierError(routineName)
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
      throw QueryCompilationErrors.fileTypeInRoutineSignatureUnsupportedMapKeyError(routineName)
    }
  }

  /**
   * Checks the return type of a function that is not created by SQL DDL, for example a UDF
   * registered through `spark.udf.register`, as [[checkNoQualifierInRoutineSignature]] and
   * [[checkNoFileTypeMapKeyInReturnType]] do for SQL functions. `routineName` is the unquoted
   * name of the function. Does nothing when the FILE type is disabled.
   */
  def checkNoQualifierInRegisteredRoutineReturnType(
      returnType: DataType,
      routineName: String): Unit = {
    if (SQLConf.get.isFileTypeEnabled) {
      val quotedName = toSQLId(Seq(routineName))
      checkNoQualifierInRoutineSignature(Seq(returnType), quotedName)
      checkNoFileTypeMapKeyInReturnType(returnType, quotedName)
    }
  }
}
