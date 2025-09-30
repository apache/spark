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
import org.apache.spark.sql.types._

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

    val resolved = resolveColumns(tableName, query.output, expected,
      conf, byName, supportColDefaultValue)

    if (resolved == query.output) {
      query
    } else {
      Project(resolved, query)
    }
  }

  def resolveField(
      tableName: String,
      value: Expression,
      col: Attribute,
      byName: Boolean,
      conf: SQLConf,
      colPath: Seq[String]): NamedExpression = {
    val resolved = (value.dataType, col.dataType) match {
      // no need to reorder inner fields or cast if types are already compatible
      case (valueType, colType) if DataType.equalsIgnoreCompatibleNullability(valueType, colType) =>
        verifyCanWrite(tableName, valueType, colType, byName, conf, colPath)
        withNullabilityChecked(value, col, conf, colPath)
      case (valueType: StructType, colType: StructType) =>
        resolveStructType(tableName, value, valueType, col, colType, byName, conf, colPath)
      case (valueType: ArrayType, colType: ArrayType) =>
        resolveArrayType(tableName, value, valueType, col, colType, byName, conf, colPath)
      case (valueType: MapType, colType: MapType) =>
        resolveMapType(tableName, value, valueType, col, colType, byName, conf, colPath)
      case _ =>
        resolveBasicType(tableName, value, col, conf, colPath)
    }

    applyColumnMetadata(resolved, col)
  }

  private def resolveBasicType(
      tableName: String,
      value: Expression,
      attr: Attribute,
      conf: SQLConf,
      colPath: Seq[String]): Expression = {

    val attrTypeHasCharVarchar = CharVarcharUtils.hasCharVarchar(attr.dataType)
    val attrTypeWithoutCharVarchar = if (attrTypeHasCharVarchar) {
      CharVarcharUtils.replaceCharVarcharWithString(attr.dataType)
    } else {
      attr.dataType
    }

    verifyCanWrite(tableName, value.dataType, attrTypeWithoutCharVarchar, byName = true,
      conf, colPath)

    var expr = unwrapUDT(value)
    expr = withNullabilityChecked(expr, attr, conf, colPath)
    if (!DataTypeUtils.sameType(attr.dataType, expr.dataType)) {
      expr = cast(expr, attrTypeWithoutCharVarchar, conf, colPath.quoted)
      if (!conf.charVarcharAsString && attrTypeHasCharVarchar) {
        expr = CharVarcharUtils.stringLengthCheck(expr, attr.dataType)
      }
    }
    expr
  }


  /**
   * Add an [[Alias]] with the name and metadata from the given target table attribute.
   *
   * The metadata may be used by writers to get certain table properties.
   * For example [[org.apache.spark.sql.catalyst.json.JacksonGenerator]]
   * looks for default value metadata to control some behavior.
   * This is not the best design, but it is the way at this time.
   * We should change all the writers to pick up table configuration
   * from the table directly. However, there are many third-party
   * connectors that may rely on this behavior.
   *
   * We also must remove any [[CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY]]
   * metadata from flowing out the top of the query.
   * If we don't do this, the write operation will remain unresolved, or worse
   * it may flip from resolved to unresolved.  We assume that the read-side
   * handling is performed lower in the query.
   *
   * Moreover, we cannot propagate other source metadata, like source table
   * default value definitions without confusing writers with reader metadata.
   * So we need to be sure we block the source metadata from propagating.
   *
   * See SPARK-52772 for a discussion on rewrites that caused trouble with
   * going from resolved to unresolved.
   *
   * There are some rewrites the undo our efforts to set the metadata (see below).
   * If we return the two inherited childMetadata, this may cause some
   * writers to behave incorrectly, or if the childMetadata has
   * the char/varchar key, then the update will flip from resolved back to unresolved,
   * which causes validateOptimizedPlan to throw exception.
   *
   * So we choose representations that:
   *   - keep the table attr metadata when nonempty
   *   - never keeps the char/varchar key
   *   - may leak the query meta
   */
  private def applyColumnMetadata(expr: Expression, column: Attribute): NamedExpression = {
    // We have dealt with the required write-side char/varchar processing.
    // We do not want to transfer that information to the read-side.
    // If we do, the write operation will fail to resolve.
    val requiredMetadata = CharVarcharUtils.cleanMetadata(column.metadata)

    // Make sure that the result has the requiredMetadata and only that.
    //
    // If the expr is a NamedLambdaVariable, it must be from our handling of structured
    // array or map fields; the Alias will be added on the outer structured value.
    //
    // Even an Attribute with the proper name and metadata is not enough to prevent
    // source query metadata leaking to the Write after rewrites, ie:
    //   case a: Attribute if a.name == column.name && a.metadata == requiredMetadata => a
    //
    // The problem is that an Attribute can be replaced by what it refers to, for example:
    //    Project AttrRef(metadata={}, exprId=2)
    //      Project Alias(
    //         cast(AttrRef(metadata={source_field_default_value}, exprId=1) as same_type),
    //         exprId=2,
    //         explicitMetadata=None) -- metadata.isEmpty
    // gets rewritten to:
    //      Project Alias(
    //         AttrRef(metadata={source_field_default_value}, exprId=1),
    //         exprId=2,
    //         explicitMetadata=None) -- metadata.nonEmpty !!
    //
    // So we always add an Alias(expr, name, explicitMetadata = Some(requiredMetadata))
    // to prevent expr from leaking the source query metadata into the Write.
    expr match {
      case v: NamedLambdaVariable if v.name == column.name && v.metadata == requiredMetadata =>
        v
      case _ =>
        // We cannot keep an Alias with the correct name and metadata because the
        // metadata might be derived, and derived metadata is not stable upon rewrites.
        // eg:
        //   Alias(cast(attr, attr.dataType), n).metadata is empty =>
        //   Alias(attr, n).metadata == attr.metadata.
        val stripAlias = expr match {
          case a: Alias => a.child
          case _ => expr
        }
        Alias(stripAlias, column.name)(explicitMetadata = Some(requiredMetadata))
    }
  }

  private def verifyCanWrite(
      tableName: String,
      valueType: DataType,
      expectedType: DataType,
      byName: Boolean,
      conf: SQLConf,
      colPath: Seq[String]): Unit = {
    conf.storeAssignmentPolicy match {
      case StoreAssignmentPolicy.STRICT | StoreAssignmentPolicy.ANSI =>
        DataTypeUtils.verifyCanWrite(
          tableName, valueType, expectedType, byName, conf.resolver, colPath.quoted,
          conf.storeAssignmentPolicy)
      case StoreAssignmentPolicy.LEGACY =>
        // LEGACY mode just adds Casts blindly, and Spark will only fail
        // if the type is not castable. The error is the same as a user-specified CAST.
    }
  }

  private def resolveColumns(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      byName: Boolean,
      fillDefaultValue: Boolean,
      colPath: Seq[String] = Nil): Seq[NamedExpression] = {
    val actualExpectedCols = expectedCols.map { c =>
      CharVarcharUtils.getRawType(c.metadata).map(c.withDataType).getOrElse(c)
    }

    val resolved = if (byName) {
      reorderColumnsByName(tableName, inputCols, actualExpectedCols, conf,
        colPath, fillDefaultValue)
    } else {
      resolveColumnsByPosition(tableName, inputCols, actualExpectedCols, conf, colPath)
    }
    assert(resolved.length == expectedCols.length,
      s"expected number of resolved byName=$byName columns" +
          s"${expectedCols.length} != ${resolved.length}")

    resolved
  }

  private def reorderColumnsByName(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      colPath: Seq[String] = Nil,
      fillDefaultValue: Boolean = false): Seq[NamedExpression] = {
    val matchedCols = mutable.HashSet.empty[String]
    val reordered = expectedCols.map { expectedCol =>
      val matched = inputCols.filter(col => conf.resolver(col.name, expectedCol.name))
      val newColPath = colPath :+ expectedCol.name
      if (matched.isEmpty) {
        val defaultExpr = if (fillDefaultValue) {
          getDefaultValueExprOrNullLit(expectedCol, conf.useNullsForMissingDefaultColumnValues)
        } else {
          None
        }
        defaultExpr.getOrElse {
          throw QueryCompilationErrors.incompatibleDataToTableCannotFindDataError(
            tableName, newColPath.quoted
          )
        }
        applyColumnMetadata(defaultExpr.get, expectedCol)
      } else if (matched.length > 1) {
        throw QueryCompilationErrors.incompatibleDataToTableAmbiguousColumnNameError(
          tableName, newColPath.quoted
        )
      } else {
        matchedCols += matched.head.name
        val matchedCol = matched.head
        resolveField(tableName, matchedCol, expectedCol, byName = true, conf, newColPath)
      }
    }

    if (matchedCols.size < inputCols.length) {
      val extraCols = columnsToString(inputCols.filterNot(col => matchedCols.contains(col.name)))
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
  }

  private def resolveColumnsByPosition(
      tableName: String,
      inputCols: Seq[NamedExpression],
      expectedCols: Seq[Attribute],
      conf: SQLConf,
      colPath: Seq[String] = Nil): Seq[NamedExpression] = {
    if (inputCols.size > expectedCols.size) {
      val extraColsStr = columnsToString(inputCols.drop(expectedCols.size))
      if (colPath.isEmpty) {
        throw QueryCompilationErrors.cannotWriteTooManyColumnsToTableError(tableName,
          expectedCols.map(_.name), inputCols.map(_.toAttribute))
      } else {
        throw QueryCompilationErrors.incompatibleDataToTableExtraStructFieldsError(
          tableName, colPath.quoted, extraColsStr
        )
      }
    } else if (inputCols.size < expectedCols.size) {
      val missingColsStr = columnsToString(expectedCols.drop(inputCols.size))
      if (colPath.isEmpty) {
        throw QueryCompilationErrors.cannotWriteNotEnoughColumnsToTableError(tableName,
          expectedCols.map(_.name), inputCols.map(_.toAttribute))
      } else {
        throw QueryCompilationErrors.incompatibleDataToTableStructMissingFieldsError(
          tableName, colPath.quoted, missingColsStr
        )
      }
    }

    inputCols.zip(expectedCols).map { case (inputCol, expectedCol) =>
      val newColPath = colPath :+ expectedCol.name
      resolveField(tableName, inputCol, expectedCol, byName = false, conf, newColPath)
    }
  }

  private def columnsToString(cols: Seq[NamedExpression]): String =
    cols.map(col => toSQLId(col.name)).mkString(", ")


  private[sql] def withNullabilityChecked(
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
      colPath: Seq[String]): Expression = {
    val nullCheckedInput = withNullabilityChecked(input, expected, conf, colPath)
    val fields = inputType.zipWithIndex.map { case (f, i) =>
      // todo: this blows up input evaluation by each field!
      Alias(GetStructField(nullCheckedInput, i, Some(f.name)), f.name)()
    }
    val resolved = resolveColumns(tableName, fields, toAttributes(expectedType),
      conf, byName, fillDefaultValue = false, colPath)

    // This may reconstruct a struct unnecessarily, but hopefully it gets cleaned up by rewrites.
    val struct = CreateStruct(resolved)
    if (nullCheckedInput.nullable) {
      If(IsNull(nullCheckedInput), Literal(null, struct.dataType), struct)
    } else {
      struct
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
      colPath: Seq[String]): Expression = {
    val nullCheckedInput = withNullabilityChecked(input, expected, conf, colPath)
    val param = NamedLambdaVariable("element", inputType.elementType, inputType.containsNull)
    val fakeAttr =
      AttributeReference("element", expectedType.elementType, expectedType.containsNull)()
    val res = resolveColumns(tableName, Seq(param), Seq(fakeAttr),
      conf, byName, fillDefaultValue = false, colPath)
    if (res.head == param) {
      // If the element type is the same, we can reuse the input array directly.
      nullCheckedInput
    } else {
      // todo: can't we just use a cast here?
      val func = LambdaFunction(res.head, Seq(param))
      ArrayTransform(nullCheckedInput, func)
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
      colPath: Seq[String]): Expression = {
    val nullCheckedInput = withNullabilityChecked(input, expected, conf, colPath)

    val keyParam = NamedLambdaVariable("key", inputType.keyType, nullable = false)
    val fakeKeyAttr = AttributeReference("key", expectedType.keyType, nullable = false)()
    val resKey = resolveColumns(tableName, Seq(keyParam), Seq(fakeKeyAttr),
      conf, byName, fillDefaultValue = false, colPath)

    val valueParam =
      NamedLambdaVariable("value", inputType.valueType, inputType.valueContainsNull)
    val fakeValueAttr =
      AttributeReference("value", expectedType.valueType, expectedType.valueContainsNull)()
    val resValue = resolveColumns(tableName, Seq(valueParam), Seq(fakeValueAttr),
      conf, byName, fillDefaultValue = false, colPath)

    // If the key and value expressions have not changed, we just check original map field.
    // Otherwise, we construct a new map by adding transformations to the keys and values.
    if (resKey.head == keyParam && resValue.head == valueParam) {
      nullCheckedInput
    } else {
      // todo: can't we use a cast or cast-like function instead of the transforms?
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
      MapFromArrays(newKeys, newValues)
    }
  }

  // For table insertions, capture the overflow errors and show proper message.
  // Without this method, the overflow errors of castings will show hints for turning off ANSI SQL
  // mode, which are not helpful since the behavior is controlled by the store assignment policy.
  private def checkCastOverflowInTableInsert(cast: Cast, columnName: String): Expression = {
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

  private def unwrapUDT(expr: Expression): Expression = {
    if (!expr.dataType.existsRecursively(_.isInstanceOf[UserDefinedType[_]])) {
      // todo: this is now N^2 in the type depth.  We could instead check the
      //       return value is eq to expr.
      expr
    } else {
      expr.dataType match {
        case ArrayType(et, containsNull) =>
          val param = NamedLambdaVariable("element", et, containsNull)
          val func = LambdaFunction(unwrapUDT(param), Seq(param))
          ArrayTransform(expr, func)

        case MapType(kt, vt, valueContainsNull) =>
          val keyParam = NamedLambdaVariable("key", kt, nullable = false)
          val valueParam = NamedLambdaVariable("value", vt, valueContainsNull)
          val keyFunc = LambdaFunction(unwrapUDT(keyParam), Seq(keyParam))
          val valueFunc = LambdaFunction(unwrapUDT(valueParam), Seq(valueParam))
          // This duplicates expr evaluation
          val newKeys = ArrayTransform(MapKeys(expr), keyFunc)
          val newValues = ArrayTransform(MapValues(expr), valueFunc)
          MapFromArrays(newKeys, newValues)

        case st: StructType =>
          val struct = CreateNamedStruct(st.zipWithIndex.flatMap { case (field, i) =>
            // todo: this blows up the expr evaluation by num fields
            val fieldExpr = unwrapUDT(GetStructField(expr, i))
            Seq(Literal(field.name), fieldExpr)
          })
          if (expr.nullable) {
            // todo: this adds yet another expr evaluation.
            If(IsNull(expr), Literal(null, struct.dataType), struct)
          } else {
            struct
          }

        case _: UserDefinedType[_] => UnwrapUDT(expr)

        case _ => expr
      }
    }
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
