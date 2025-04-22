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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns.getDefaultValueExprOrNullLit
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.StoreAssignmentPolicy
import org.apache.spark.sql.types.{ArrayType, DataType, DecimalType, IntegralType, MapType, StructType, UserDefinedType}

object TableOutputResolver extends SQLConfHelper with Logging {

  def resolveVariableOutputColumns(
      expected: Seq[VariableReference],
      query: LogicalPlan,
      conf: SQLConf): LogicalPlan = {

    if (expected.size != query.output.size) {
      throw new AnalysisException(
        errorClass = "ASSIGNMENT_ARITY_MISMATCH",
        messageParameters = Map(
          "numTarget" -> expected.size.toString,
          "numExpr" -> query.output.size.toString))
    }

    val resolved: Seq[NamedExpression] = {
      query.output.zip(expected).map { case (inputCol, expected) =>
        if (DataTypeUtils.sameType(inputCol.dataType, expected.dataType)) {
          inputCol
        } else {
          // SET VAR always uses the ANSI store assignment policy
          val cast = Cast(
            inputCol,
            expected.dataType,
            Option(conf.sessionLocalTimeZone),
            ansiEnabled = true)
          Alias(cast, expected.identifier.name)()
        }
      }
    }

    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
    }
  }

  def resolveOutputColumns(
      tableName: String,
      expected: Seq[Attribute],
      query: LogicalPlan,
      byName: Boolean,
      conf: SQLConf,
      supportColDefaultValue: Boolean = false): LogicalPlan = {

    if (expected.size < query.output.size) {
      throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(
        tableName, expected.map(_.name), query.output)
    }

    val errors = new mutable.ArrayBuffer[String]()
    val resolved: Seq[NamedExpression] = if (byName) {
      // If a top-level column does not have a corresponding value in the input query, fill with
      // the column's default value. We need to pass `fillDefaultValue` as true here, if the
      // `supportColDefaultValue` parameter is also true.
      reorderColumnsByName(
        tableName,
        query.output,
        expected,
        conf,
        errors += _,
        fillDefaultValue = supportColDefaultValue)
    } else {
      if (expected.size > query.output.size) {
        throw QueryCompilationErrors.cannotWriteNotEnoughColumnsToTableError(
          tableName, expected.map(_.name), query.output)
      }
      resolveColumnsByPosition(tableName, query.output, expected, conf, errors += _)
    }

    if (errors.nonEmpty) {
      throw QueryCompilationErrors.incompatibleDataToTableCannotFindDataError(
        tableName, expected.map(_.name).map(toSQLId).mkString(", "))
    }

    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
    }
  }

  def resolveUpdate(
      tableName: String,
      value: Expression,
      col: Attribute,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Expression = {

    (value.dataType, col.dataType) match {
      // no need to reorder inner fields or cast if types are already compatible
      case (valueType, colType) if DataType.equalsIgnoreCompatibleNullability(valueType, colType) =>
        val canWriteExpr = canWrite(
          tableName, valueType, colType, byName = true, conf, addError, colPath)
        if (canWriteExpr) checkNullability(value, col, conf, colPath) else value
      case (valueType: StructType, colType: StructType) =>
        val resolvedValue = resolveStructType(
          tableName, value, valueType, col, colType,
          byName = true, conf, addError, colPath)
        resolvedValue.getOrElse(value)
      case (valueType: ArrayType, colType: ArrayType) =>
        val resolvedValue = resolveArrayType(
          tableName, value, valueType, col, colType,
          byName = true, conf, addError, colPath)
        resolvedValue.getOrElse(value)
      case (valueType: MapType, colType: MapType) =>
        val resolvedValue = resolveMapType(
          tableName, value, valueType, col, colType,
          byName = true, conf, addError, colPath)
        resolvedValue.getOrElse(value)
      case _ =>
        checkUpdate(tableName, value, col, conf, addError, colPath)
    }
  }

  private def checkUpdate(
      tableName: String,
      value: Expression,
      attr: Attribute,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Expression = {

    val attrTypeHasCharVarchar = CharVarcharUtils.hasCharVarchar(attr.dataType)
    val attrTypeWithoutCharVarchar = if (attrTypeHasCharVarchar) {
      CharVarcharUtils.replaceCharVarcharWithString(attr.dataType)
    } else {
      attr.dataType
    }

    val canWriteValue = canWrite(
      tableName, value.dataType, attrTypeWithoutCharVarchar,
      byName = true, conf, addError, colPath)

    if (canWriteValue) {
      val nullCheckedValue = checkNullability(value, attr, conf, colPath)
      val casted = cast(nullCheckedValue, attrTypeWithoutCharVarchar, conf, colPath.quoted)
      val exprWithStrLenCheck = if (conf.charVarcharAsString || !attrTypeHasCharVarchar) {
        casted
      } else {
        CharVarcharUtils.stringLengthCheck(casted, attr.dataType)
      }
      Alias(exprWithStrLenCheck, attr.name)(explicitMetadata = Some(attr.metadata))
    } else {
      value
    }
  }

  private def canWrite(
      tableName: String,
      valueType: DataType,
      expectedType: DataType,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Boolean = {
    conf.storeAssignmentPolicy match {
      case StoreAssignmentPolicy.STRICT | StoreAssignmentPolicy.ANSI =>
        DataTypeUtils.canWrite(
          tableName, valueType, expectedType, byName, conf.resolver, colPath.quoted,
          conf.storeAssignmentPolicy, addError)
      case _ =>
        true
    }
  }

  private def reorderColumnsByName(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String] = Nil,
      fillDefaultValue: Boolean = false): Seq[NamedExpression] = {
    val matchedCols = mutable.HashSet.empty[String]
    val reordered = expectedCols.flatMap { expectedCol =>
      val matched = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
      val newColPath = colPath :+ expectedCol.name
      if (matched.isEmpty) {
        val defaultExpr = if (fillDefaultValue) {
          getDefaultValueExprOrNullLit(expectedCol, conf.useNullsForMissingDefaultColumnValues)
        } else {
          None
        }
        if (defaultExpr.isEmpty) {
          throw QueryCompilationErrors.incompatibleDataToTableCannotFindDataError(
            tableName, newColPath.quoted
          )
        }
        defaultExpr
      } else if (matched.length > 1) {
        throw QueryCompilationErrors.incompatibleDataToTableAmbiguousColumnNameError(
          tableName, newColPath.quoted
        )
      } else {
        matchedCols += matched.head.name
        val expectedName = expectedCol.name
        val matchedCol = matched.head match {
          // Save an Alias if we can change the name directly.
          case a: Attribute => a.withName(expectedName)
          case a: Alias => a.withName(expectedName)
          case other => other
        }
        val actualExpectedCol = expectedCol.withDataType {
          CharVarcharUtils.getRawType(expectedCol.metadata).getOrElse(expectedCol.dataType)
        }
        (matchedCol.dataType, actualExpectedCol.dataType) match {
          case (matchedType: StructType, expectedType: StructType) =>
            resolveStructType(
              tableName, matchedCol, matchedType, actualExpectedCol, expectedType,
              byName = true, conf, addError, newColPath)
          case (matchedType: ArrayType, expectedType: ArrayType) =>
            resolveArrayType(
              tableName, matchedCol, matchedType, actualExpectedCol, expectedType,
              byName = true, conf, addError, newColPath)
          case (matchedType: MapType, expectedType: MapType) =>
            resolveMapType(
              tableName, matchedCol, matchedType, actualExpectedCol, expectedType,
              byName = true, conf, addError, newColPath)
          case _ =>
            checkField(
              tableName, actualExpectedCol, matchedCol, byName = true, conf, addError, newColPath)
        }
      }
    }

    if (reordered.length == expectedCols.length) {
      if (matchedCols.size < inputCols.length) {
        val extraCols = inputCols.filterNot(col => matchedCols.contains(col.name))
          .map(col => s"${toSQLId(col.name)}").mkString(", ")
        if (colPath.isEmpty) {
          throw QueryCompilationErrors.incompatibleDataToTableExtraColumnsError(tableName,
            extraCols)
        } else {
          throw QueryCompilationErrors.incompatibleDataToTableExtraStructFieldsError(
            tableName, colPath.quoted, extraCols)
        }
      } else {
        reordered
      }
    } else {
      Nil
    }
  }

  private def resolveColumnsByPosition(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String] = Nil): Seq[NamedExpression] = {
    val actualExpectedCols = expectedCols.map { attr =>
      attr.withDataType { CharVarcharUtils.getRawType(attr.metadata).getOrElse(attr.dataType) }
    }
    if (inputCols.size > actualExpectedCols.size) {
      val extraColsStr = inputCols.takeRight(inputCols.size - actualExpectedCols.size)
        .map(col => toSQLId(col.name))
        .mkString(", ")
      if (colPath.isEmpty) {
        throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(tableName,
          actualExpectedCols.map(_.name), inputCols.map(_.toAttribute))
      } else {
        throw QueryCompilationErrors.incompatibleDataToTableExtraStructFieldsError(
          tableName, colPath.quoted, extraColsStr
        )
      }
    } else if (inputCols.size < actualExpectedCols.size) {
      val missingColsStr = actualExpectedCols.takeRight(actualExpectedCols.size - inputCols.size)
        .map(col => toSQLId(col.name))
        .mkString(", ")
      if (colPath.isEmpty) {
        throw QueryCompilationErrors.cannotWriteNotEnoughColumnsToTableError(tableName,
          actualExpectedCols.map(_.name), inputCols.map(_.toAttribute))
      } else {
        throw QueryCompilationErrors.incompatibleDataToTableStructMissingFieldsError(
          tableName, colPath.quoted, missingColsStr
        )
      }
    }

    inputCols.zip(actualExpectedCols).flatMap { case (inputCol, expectedCol) =>
      val newColPath = colPath :+ expectedCol.name
      (inputCol.dataType, expectedCol.dataType) match {
        case (inputType: StructType, expectedType: StructType) =>
          resolveStructType(
            tableName, inputCol, inputType, expectedCol, expectedType,
            byName = false, conf, addError, newColPath)
        case (inputType: ArrayType, expectedType: ArrayType) =>
          resolveArrayType(
            tableName, inputCol, inputType, expectedCol, expectedType,
            byName = false, conf, addError, newColPath)
        case (inputType: MapType, expectedType: MapType) =>
          resolveMapType(
            tableName, inputCol, inputType, expectedCol, expectedType,
            byName = false, conf, addError, newColPath)
        case _ =>
          checkField(tableName, expectedCol, inputCol, byName = false, conf, addError, newColPath)
      }
    }
  }

  private[sql] def checkNullability(
      input: Expression,
      expected: Attribute,
      conf: SQLConf,
      colPath: Seq[String]): Expression = {
    if (requiresNullChecks(input, expected, conf)) {
      AssertNotNull(input, colPath)
    } else {
      input
    }
  }

  private def requiresNullChecks(
      input: Expression,
      attr: Attribute,
      conf: SQLConf): Boolean = {
    input.nullable && !attr.nullable && conf.storeAssignmentPolicy != StoreAssignmentPolicy.LEGACY
  }

  private def resolveStructType(
      tableName: String,
      input: Expression,
      inputType: StructType,
      expected: Attribute,
      expectedType: StructType,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {
    val nullCheckedInput = checkNullability(input, expected, conf, colPath)
    val fields = inputType.zipWithIndex.map { case (f, i) =>
      Alias(GetStructField(nullCheckedInput, i, Some(f.name)), f.name)()
    }
    val resolved = if (byName) {
      reorderColumnsByName(tableName, fields, toAttributes(expectedType), conf, addError, colPath)
    } else {
      resolveColumnsByPosition(
        tableName, fields, toAttributes(expectedType), conf, addError, colPath)
    }
    if (resolved.length == expectedType.length) {
      val struct = CreateStruct(resolved)
      val res = if (nullCheckedInput.nullable) {
        If(IsNull(nullCheckedInput), Literal(null, struct.dataType), struct)
      } else {
        struct
      }
      Some(Alias(res, expected.name)())
    } else {
      None
    }
  }

  private def resolveArrayType(
      tableName: String,
      input: Expression,
      inputType: ArrayType,
      expected: Attribute,
      expectedType: ArrayType,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {
    val nullCheckedInput = checkNullability(input, expected, conf, colPath)
    val param = NamedLambdaVariable("element", inputType.elementType, inputType.containsNull)
    val fakeAttr =
      AttributeReference("element", expectedType.elementType, expectedType.containsNull)()
    val res = if (byName) {
      reorderColumnsByName(tableName, Seq(param), Seq(fakeAttr), conf, addError, colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(param), Seq(fakeAttr), conf, addError, colPath)
    }
    if (res.length == 1) {
      if (res.head == param) {
        // If the element type is the same, we can reuse the input array directly.
        Some(
          Alias(nullCheckedInput, expected.name)(
            nonInheritableMetadataKeys =
              Seq(CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)))
      } else {
        val func = LambdaFunction(res.head, Seq(param))
        Some(Alias(ArrayTransform(nullCheckedInput, func), expected.name)())
      }
    } else {
      None
    }
  }

  private def resolveMapType(
      tableName: String,
      input: Expression,
      inputType: MapType,
      expected: Attribute,
      expectedType: MapType,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {
    val nullCheckedInput = checkNullability(input, expected, conf, colPath)

    val keyParam = NamedLambdaVariable("key", inputType.keyType, nullable = false)
    val fakeKeyAttr = AttributeReference("key", expectedType.keyType, nullable = false)()
    val resKey = if (byName) {
      reorderColumnsByName(tableName, Seq(keyParam), Seq(fakeKeyAttr), conf, addError, colPath)
    } else {
      resolveColumnsByPosition(tableName, Seq(keyParam), Seq(fakeKeyAttr), conf, addError, colPath)
    }

    val valueParam =
      NamedLambdaVariable("value", inputType.valueType, inputType.valueContainsNull)
    val fakeValueAttr =
      AttributeReference("value", expectedType.valueType, expectedType.valueContainsNull)()
    val resValue = if (byName) {
      reorderColumnsByName(tableName, Seq(valueParam), Seq(fakeValueAttr), conf, addError, colPath)
    } else {
      resolveColumnsByPosition(
        tableName, Seq(valueParam), Seq(fakeValueAttr), conf, addError, colPath)
    }

    if (resKey.length == 1 && resValue.length == 1) {
      // If the key and value expressions have not changed, we just check original map field.
      // Otherwise, we construct a new map by adding transformations to the keys and values.
      if (resKey.head == keyParam && resValue.head == valueParam) {
        Some(
          Alias(nullCheckedInput, expected.name)(
            nonInheritableMetadataKeys =
              Seq(CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)))
      } else {
        val newKeys = if (resKey.head != keyParam) {
          val keyFunc = LambdaFunction(resKey.head, Seq(keyParam))
          ArrayTransform(MapKeys(nullCheckedInput), keyFunc)
        } else {
          MapKeys(nullCheckedInput)
        }
        val newValues = if (resValue.head != valueParam) {
          val valueFunc = LambdaFunction(resValue.head, Seq(valueParam))
          ArrayTransform(MapValues(nullCheckedInput), valueFunc)
        } else {
          MapValues(nullCheckedInput)
        }
        Some(Alias(MapFromArrays(newKeys, newValues), expected.name)())
      }
    } else {
      None
    }
  }

  // For table insertions, capture the overflow errors and show proper message.
  // Without this method, the overflow errors of castings will show hints for turning off ANSI SQL
  // mode, which are not helpful since the behavior is controlled by the store assignment policy.
  def checkCastOverflowInTableInsert(cast: Cast, columnName: String): Expression = {
    if (canCauseCastOverflow(cast)) {
      CheckOverflowInTableInsert(cast, columnName)
    } else {
      cast
    }
  }

  def suitableForByNameCheck(
      byName: Boolean,
      expected: Seq[Attribute],
      queryOutput: Seq[Attribute]): Unit = {
    if (!byName && expected.size == queryOutput.size &&
      expected.forall(e => queryOutput.exists(p => conf.resolver(p.name, e.name))) &&
      expected.zip(queryOutput).exists(e => !conf.resolver(e._1.name, e._2.name))) {
      logWarning("The query columns and the table columns have same names but different " +
        "orders. You can use INSERT [INTO | OVERWRITE] BY NAME to reorder the query columns to " +
        "align with the table columns.")
    }
  }

  private def containsIntegralOrDecimalType(dt: DataType): Boolean = dt match {
    case _: IntegralType | _: DecimalType => true
    case a: ArrayType => containsIntegralOrDecimalType(a.elementType)
    case m: MapType =>
      containsIntegralOrDecimalType(m.keyType) || containsIntegralOrDecimalType(m.valueType)
    case s: StructType =>
      s.fields.exists(sf => containsIntegralOrDecimalType(sf.dataType))
    case _ => false
  }

  private def canCauseCastOverflow(cast: Cast): Boolean = {
    containsIntegralOrDecimalType(cast.dataType) &&
      !Cast.canUpCast(cast.child.dataType, cast.dataType)
  }

  private def isCompatible(tableAttr: Attribute, queryExpr: NamedExpression): Boolean = {
    DataTypeUtils.sameType(tableAttr.dataType, queryExpr.dataType) &&
      tableAttr.name == queryExpr.name &&
      tableAttr.metadata == queryExpr.metadata
  }

  private def checkField(
      tableName: String,
      tableAttr: Attribute,
      queryExpr: NamedExpression,
      byName: Boolean,
      conf: SQLConf,
      addError: String => Unit,
      colPath: Seq[String]): Option[NamedExpression] = {

    val attrTypeHasCharVarchar = CharVarcharUtils.hasCharVarchar(tableAttr.dataType)
    val attrTypeWithoutCharVarchar = if (attrTypeHasCharVarchar) {
      CharVarcharUtils.replaceCharVarcharWithString(tableAttr.dataType)
    } else {
      tableAttr.dataType
    }
    lazy val outputField = if (isCompatible(tableAttr, queryExpr)) {
      if (requiresNullChecks(queryExpr, tableAttr, conf)) {
        val assert = AssertNotNull(queryExpr, colPath)
        Some(Alias(assert, tableAttr.name)(explicitMetadata = Some(tableAttr.metadata)))
      } else {
        Some(queryExpr)
      }
    } else {
      val nullCheckedQueryExpr = checkNullability(queryExpr, tableAttr, conf, colPath)
      val udtUnwrapped = unwrapUDT(nullCheckedQueryExpr)
      val casted = cast(udtUnwrapped, attrTypeWithoutCharVarchar, conf, colPath.quoted)
      val exprWithStrLenCheck = if (conf.charVarcharAsString || !attrTypeHasCharVarchar) {
        casted
      } else {
        CharVarcharUtils.stringLengthCheck(casted, tableAttr.dataType)
      }
      // Renaming is needed for handling the following cases like
      // 1) Column names/types do not match, e.g., INSERT INTO TABLE tab1 SELECT 1, 2
      // 2) Target tables have column metadata
      Some(Alias(exprWithStrLenCheck, tableAttr.name)(explicitMetadata = Some(tableAttr.metadata)))
    }

    val canWriteExpr = canWrite(
      tableName, queryExpr.dataType, attrTypeWithoutCharVarchar,
      byName, conf, addError, colPath)

    if (canWriteExpr) outputField else None
  }

  private def unwrapUDT(expr: Expression): Expression = expr.dataType match {
    case ArrayType(et, containsNull) =>
      val param = NamedLambdaVariable("element", et, containsNull)
      val func = LambdaFunction(unwrapUDT(param), Seq(param))
      ArrayTransform(expr, func)

    case MapType(kt, vt, valueContainsNull) =>
      val keyParam = NamedLambdaVariable("key", kt, nullable = false)
      val valueParam = NamedLambdaVariable("value", vt, valueContainsNull)
      val keyFunc = LambdaFunction(unwrapUDT(keyParam), Seq(keyParam))
      val valueFunc = LambdaFunction(unwrapUDT(valueParam), Seq(valueParam))
      val newKeys = ArrayTransform(MapKeys(expr), keyFunc)
      val newValues = ArrayTransform(MapValues(expr), valueFunc)
      MapFromArrays(newKeys, newValues)

    case st: StructType =>
      val newFieldExprs = st.indices.map { i =>
        unwrapUDT(GetStructField(expr, i))
      }
      val struct = CreateNamedStruct(st.zip(newFieldExprs).flatMap {
        case (field, newExpr) => Seq(Literal(field.name), newExpr)
      })
      if (expr.nullable) {
        If(IsNull(expr), Literal(null, struct.dataType), struct)
      } else {
        struct
      }

    case _: UserDefinedType[_] => UnwrapUDT(expr)

    case _ => expr
  }

  private def cast(
      expr: Expression,
      expectedType: DataType,
      conf: SQLConf,
      colName: String): Expression = {

    conf.storeAssignmentPolicy match {
      case StoreAssignmentPolicy.ANSI =>
        val cast = Cast(expr, expectedType, Option(conf.sessionLocalTimeZone), ansiEnabled = true)
        cast.setTagValue(Cast.BY_TABLE_INSERTION, ())
        checkCastOverflowInTableInsert(cast, colName)

      case StoreAssignmentPolicy.LEGACY =>
        Cast(expr, expectedType, Option(conf.sessionLocalTimeZone), ansiEnabled = false)

      case _ =>
        Cast(expr, expectedType, Option(conf.sessionLocalTimeZone))
    }
  }
}
