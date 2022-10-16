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

import org.apache.spark.sql.catalyst.analysis.{TypeCheckResult, TypeCoercion}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLType
import org.apache.spark.sql.catalyst.expressions.RowOrdering
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

/**
 * Functions to help with checking for valid data types and value comparison of various types.
 */
object TypeUtils {

  def checkForOrderingExpr(dt: DataType, caller: String): TypeCheckResult = {
    if (RowOrdering.isOrderable(dt)) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      DataTypeMismatch(
        errorSubClass = "INVALID_ORDERING_TYPE",
        Map(
          "functionName" -> caller,
          "dataType" -> toSQLType(dt)
        )
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
          "functionName" -> caller,
          "dataType" -> types.map(toSQLType).mkString("(", " or ", ")")
        )
      )
    }
  }

  def checkForMapKeyType(keyType: DataType): TypeCheckResult = {
    if (keyType.existsRecursively(_.isInstanceOf[MapType])) {
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

  def checkForAnsiIntervalOrNumericType(
      dt: DataType, funcName: String): TypeCheckResult = dt match {
    case _: AnsiIntervalType | NullType =>
      TypeCheckResult.TypeCheckSuccess
    case dt if dt.isInstanceOf[NumericType] => TypeCheckResult.TypeCheckSuccess
    case other => TypeCheckResult.TypeCheckFailure(
      s"function $funcName requires numeric or interval types, not ${other.catalogString}")
  }

  def getNumeric(t: DataType, exactNumericRequired: Boolean = false): Numeric[Any] = {
    if (exactNumericRequired) {
      t.asInstanceOf[NumericType].exactNumeric.asInstanceOf[Numeric[Any]]
    } else {
      t.asInstanceOf[NumericType].numeric.asInstanceOf[Numeric[Any]]
    }
  }

  @scala.annotation.tailrec
  def getInterpretedOrdering(t: DataType): Ordering[Any] = {
    t match {
      case i: AtomicType => i.ordering.asInstanceOf[Ordering[Any]]
      case a: ArrayType => a.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case s: StructType => s.interpretedOrdering.asInstanceOf[Ordering[Any]]
      case udt: UserDefinedType[_] => getInterpretedOrdering(udt.sqlType)
    }
  }

  /**
   * Returns true if the equals method of the elements of the data type is implemented properly.
   * This also means that they can be safely used in collections relying on the equals method,
   * as sets or maps.
   */
  def typeWithProperEquals(dataType: DataType): Boolean = dataType match {
    case BinaryType => false
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
}
