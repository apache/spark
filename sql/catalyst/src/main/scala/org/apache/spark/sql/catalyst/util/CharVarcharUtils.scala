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

import scala.collection.mutable

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

object CharVarcharUtils extends Logging with SparkCharVarcharUtils {

  // visible for testing
  private[sql] val CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING"

  /**
   * Replaces CharType/VarcharType with StringType recursively in the given struct type. If a
   * top-level StructField's data type is CharType/VarcharType or has nested CharType/VarcharType,
   * this method will add the original type string to the StructField's metadata, so that we can
   * re-construct the original data type with CharType/VarcharType later when needed.
   */
  def replaceCharVarcharWithStringInSchema(st: StructType): StructType = {
    StructType(st.map { field =>
      if (hasCharVarchar(field.dataType)) {
        val metadata = new MetadataBuilder().withMetadata(field.metadata)
          .putString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY, field.dataType.catalogString).build()
        field.copy(dataType = replaceCharVarcharWithString(field.dataType), metadata = metadata)
      } else {
        field
      }
    })
  }

  /**
   * Replaces CharType with VarcharType recursively in the given data type.
   */
  def replaceCharWithVarchar(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharWithVarchar(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharWithVarchar(kt), replaceCharWithVarchar(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharWithVarchar(field.dataType))
      })
    case CharType(length) => VarcharType(length)
    case _ => dt
  }

  /**
   * Replaces CharType/VarcharType with StringType recursively in the given data type, with a
   * warning message if it has char or varchar types
   */
  def replaceCharVarcharWithStringForCast(dt: DataType): DataType = {
    if (SQLConf.get.charVarcharAsString) {
      replaceCharVarcharWithString(dt)
    } else if (hasCharVarchar(dt) && !SQLConf.get.preserveCharVarcharTypeInfo) {
      logWarning(log"The Spark cast operator does not support char/varchar type and simply treats" +
        log" them as string type. Please use string type directly to avoid confusion. Otherwise," +
        log" you can set ${MDC(CONFIG, SQLConf.LEGACY_CHAR_VARCHAR_AS_STRING.key)} " +
        log"to true, so that Spark treat them as string type as same as Spark 3.0 and earlier")
      replaceCharVarcharWithString(dt)
    } else {
      dt
    }
  }

  /**
   * Removes the metadata entry that contains the original type string of CharType/VarcharType from
   * the given attribute's metadata.
   */
  def cleanAttrMetadata(attr: AttributeReference): AttributeReference = {
    val cleaned = new MetadataBuilder().withMetadata(attr.metadata)
      .remove(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY).build()
    attr.withMetadata(cleaned)
  }

  def getRawTypeString(metadata: Metadata): Option[String] = {
    if (metadata.contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)) {
      Some(metadata.getString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY))
    } else {
      None
    }
  }

  /**
   * Re-construct the original data type from the type string in the given metadata.
   * This is needed when dealing with char/varchar columns/fields.
   */
  def getRawType(metadata: Metadata): Option[DataType] = {
    getRawTypeString(metadata).map(CatalystSqlParser.parseDataType)
  }

  /**
   * Re-construct the original schema from the type string in the given metadata of each field.
   */
  def getRawSchema(schema: StructType): StructType = {
    val fields = schema.map { field =>
      getRawType(field.metadata).map(dt => field.copy(dataType = dt)).getOrElse(field)
    }
    StructType(fields)
  }

  def getRawSchema(schema: StructType, conf: SQLConf): StructType = {
    val fields = schema.map { field =>
      getRawType(field.metadata).map { dt =>
        if (conf.getConf(SQLConf.CHAR_AS_VARCHAR)) {
          val metadata = new MetadataBuilder().withMetadata(field.metadata)
            .remove(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY).build()
          field.copy(dataType = replaceCharWithVarchar(dt), metadata = metadata)
        } else {
          field.copy(dataType = dt)
        }
      }.getOrElse(field)
    }
    StructType(fields)
  }

  /**
   * Returns an expression to apply write-side string length check for the given expression. A
   * string value can not exceed N characters if it's written into a CHAR(N)/VARCHAR(N)
   * column/field.
   */
  def stringLengthCheck(expr: Expression, targetAttr: Attribute): Expression = {
    getRawType(targetAttr.metadata).map { rawType =>
      stringLengthCheck(expr, rawType)
    }.getOrElse(expr)
  }

  def stringLengthCheck(expr: Expression, dt: DataType): Expression = {
    processStringForCharVarchar(
      expr,
      dt,
      charFuncName = Some("charTypeWriteSideCheck"),
      varcharFuncName = Some("varcharTypeWriteSideCheck"))
  }

  private def processStringForCharVarchar(
      expr: Expression,
      dt: DataType,
      charFuncName: Option[String],
      varcharFuncName: Option[String]): Expression = {
    dt match {
      case CharType(length) if charFuncName.isDefined =>
        StaticInvoke(
          classOf[CharVarcharCodegenUtils],
          if (SQLConf.get.preserveCharVarcharTypeInfo) {
            CharType(length)
          } else {
            StringType
          },
          charFuncName.get,
          expr :: Literal(length) :: Nil,
          returnNullable = false)

      case VarcharType(length) if varcharFuncName.isDefined =>
        StaticInvoke(
          classOf[CharVarcharCodegenUtils],
          if (SQLConf.get.preserveCharVarcharTypeInfo) {
            VarcharType(length)
          } else {
            StringType
          },
          varcharFuncName.get,
          expr :: Literal(length) :: Nil,
          returnNullable = false)

      case StructType(fields) =>
        val struct = CreateNamedStruct(fields.zipWithIndex.flatMap { case (f, i) =>
          Seq(Literal(f.name), processStringForCharVarchar(
            GetStructField(expr, i, Some(f.name)), f.dataType, charFuncName, varcharFuncName))
        }.toImmutableArraySeq)
        if (struct.valExprs.forall(_.isInstanceOf[GetStructField])) {
          // No field needs char/varchar processing, just return the original expression.
          expr
        } else if (expr.nullable) {
          If(IsNull(expr), Literal(null, struct.dataType), struct)
        } else {
          struct
        }

      case ArrayType(et, containsNull) =>
        processStringForCharVarcharInArray(expr, et, containsNull, charFuncName, varcharFuncName)

      case MapType(kt, vt, valueContainsNull) =>
        val keys = MapKeys(expr)
        val newKeys = processStringForCharVarcharInArray(
          keys, kt, containsNull = false, charFuncName, varcharFuncName)
        val values = MapValues(expr)
        val newValues = processStringForCharVarcharInArray(
          values, vt, valueContainsNull, charFuncName, varcharFuncName)
        if (newKeys.fastEquals(keys) && newValues.fastEquals(values)) {
          // If map key/value does not need char/varchar processing, return the original expression.
          expr
        } else {
          MapFromArrays(newKeys, newValues)
        }

      case _ => expr
    }
  }

  private def processStringForCharVarcharInArray(
      arr: Expression,
      et: DataType,
      containsNull: Boolean,
      charFuncName: Option[String],
      varcharFuncName: Option[String]): Expression = {
    val param = NamedLambdaVariable("x", replaceCharVarcharWithString(et), containsNull)
    val funcBody = processStringForCharVarchar(param, et, charFuncName, varcharFuncName)
    if (funcBody.fastEquals(param)) {
      // If array element does not need char/varchar processing, return the original expression.
      arr
    } else {
      ArrayTransform(arr, LambdaFunction(funcBody, Seq(param)))
    }
  }

  def addPaddingForScan(attr: Attribute): Expression = {
    getRawType(attr.metadata).map { rawType =>
      processStringForCharVarchar(
        attr, rawType, charFuncName = Some("readSidePadding"), varcharFuncName = None)
    }.getOrElse(attr)
  }

  /**
   * Return expressions to apply char type padding for the string comparison between the given
   * attributes. When comparing two char type columns/fields, we need to pad the shorter one to
   * the longer length.
   */
  def addPaddingInStringComparison(attrs: Seq[Attribute], alwaysPad: Boolean): Seq[Expression] = {
    val rawTypes = attrs.map(attr => getRawType(attr.metadata))
    if (rawTypes.exists(_.isEmpty)) {
      attrs
    } else {
      val typeWithTargetCharLength = rawTypes.map(_.get).reduce(typeWithWiderCharLength)
      attrs.zip(rawTypes.map(_.get)).map { case (attr, rawType) =>
        padCharToTargetLength(attr, rawType, typeWithTargetCharLength, alwaysPad).getOrElse(attr)
      }
    }
  }

  private def typeWithWiderCharLength(type1: DataType, type2: DataType): DataType = {
    (type1, type2) match {
      case (CharType(len1), CharType(len2)) =>
        CharType(math.max(len1, len2))
      case (StructType(fields1), StructType(fields2)) =>
        assert(fields1.length == fields2.length)
        StructType(fields1.zip(fields2).map { case (left, right) =>
          StructField("", typeWithWiderCharLength(left.dataType, right.dataType))
        })
      case (ArrayType(et1, _), ArrayType(et2, _)) =>
        ArrayType(typeWithWiderCharLength(et1, et2))
      case _ => NullType
    }
  }

  private def padCharToTargetLength(
      expr: Expression,
      rawType: DataType,
      typeWithTargetCharLength: DataType,
      alwaysPad: Boolean): Option[Expression] = {
    (rawType, typeWithTargetCharLength) match {
      case (CharType(len), CharType(target)) if alwaysPad || target > len =>
        Some(StringRPad(expr, Literal(target)))

      case (StructType(fields), StructType(targets)) =>
        assert(fields.length == targets.length)
        var i = 0
        var needPadding = false
        val createStructExprs = mutable.ArrayBuffer.empty[Expression]
        while (i < fields.length) {
          val field = fields(i)
          val fieldExpr = GetStructField(expr, i, Some(field.name))
          val padded = padCharToTargetLength(
            fieldExpr, field.dataType, targets(i).dataType, alwaysPad)
          needPadding = padded.isDefined
          createStructExprs += Literal(field.name)
          createStructExprs += padded.getOrElse(fieldExpr)
          i += 1
        }
        if (needPadding) Some(CreateNamedStruct(createStructExprs.toSeq)) else None

      case (ArrayType(et, containsNull), ArrayType(target, _)) =>
        val param = NamedLambdaVariable("x", replaceCharVarcharWithString(et), containsNull)
        padCharToTargetLength(param, et, target, alwaysPad).map { padded =>
          val func = LambdaFunction(padded, Seq(param))
          ArrayTransform(expr, func)
        }

      // We don't handle MapType here as it's not comparable.

      case _ => None
    }
  }
}
