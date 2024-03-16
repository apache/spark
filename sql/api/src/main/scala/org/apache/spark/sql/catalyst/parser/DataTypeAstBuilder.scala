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
package org.apache.spark.sql.catalyst.parser

import java.util.Locale

import scala.jdk.CollectionConverters._

import org.antlr.v4.runtime.Token
import org.antlr.v4.runtime.tree.ParseTree

import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.catalyst.util.SparkParserUtils.{string, withOrigin}
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, CharType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, MetadataBuilder, NullType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, VarcharType, VariantType, YearMonthIntervalType}

class DataTypeAstBuilder extends SqlBaseParserBaseVisitor[AnyRef] {
  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  override def visitSingleDataType(ctx: SingleDataTypeContext): DataType = withOrigin(ctx) {
    typedVisit[DataType](ctx.dataType)
  }

  override def visitSingleTableSchema(ctx: SingleTableSchemaContext): StructType = {
    withOrigin(ctx)(StructType(visitColTypeList(ctx.colTypeList)))
  }

  override def visitStringLit(ctx: StringLitContext): Token = {
    if (ctx != null) {
      if (ctx.STRING_LITERAL != null) {
        ctx.STRING_LITERAL.getSymbol
      } else {
        ctx.DOUBLEQUOTED_STRING.getSymbol
      }
    } else {
      null
    }
  }

  /**
   * Resolve/create a primitive type.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val typeCtx = ctx.`type`
    (typeCtx.start.getType, ctx.INTEGER_VALUE().asScala.toList) match {
      case (BOOLEAN, Nil) => BooleanType
      case (TINYINT | BYTE, Nil) => ByteType
      case (SMALLINT | SHORT, Nil) => ShortType
      case (INT | INTEGER, Nil) => IntegerType
      case (BIGINT | LONG, Nil) => LongType
      case (FLOAT | REAL, Nil) => FloatType
      case (DOUBLE, Nil) => DoubleType
      case (DATE, Nil) => DateType
      case (TIMESTAMP, Nil) => SqlApiConf.get.timestampType
      case (TIMESTAMP_NTZ, Nil) => TimestampNTZType
      case (TIMESTAMP_LTZ, Nil) => TimestampType
      case (STRING, Nil) =>
        typeCtx.children.asScala.toSeq match {
          case Seq(_) => StringType
          case Seq(_, ctx: CollateClauseContext) =>
            val collationName = visitCollateClause(ctx)
            val collationId = CollationFactory.collationNameToId(collationName)
            StringType(collationId)
        }
      case (CHARACTER | CHAR, length :: Nil) => CharType(length.getText.toInt)
      case (VARCHAR, length :: Nil) => VarcharType(length.getText.toInt)
      case (BINARY, Nil) => BinaryType
      case (DECIMAL | DEC | NUMERIC, Nil) => DecimalType.USER_DEFAULT
      case (DECIMAL | DEC | NUMERIC, precision :: Nil) =>
        DecimalType(precision.getText.toInt, 0)
      case (DECIMAL | DEC | NUMERIC, precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case (VOID, Nil) => NullType
      case (INTERVAL, Nil) => CalendarIntervalType
      case (VARIANT, Nil) => VariantType
      case (CHARACTER | CHAR | VARCHAR, Nil) =>
        throw QueryParsingErrors.charTypeMissingLengthError(ctx.`type`.getText, ctx)
      case (ARRAY | STRUCT | MAP, Nil) =>
        throw QueryParsingErrors.nestedTypeMissingElementTypeError(ctx.`type`.getText, ctx)
      case (_, params) =>
        val badType = ctx.`type`.getText
        val dtStr = if (params.nonEmpty) s"$badType(${params.mkString(",")})" else badType
        throw QueryParsingErrors.dataTypeUnsupportedError(dtStr, ctx)
    }
  }

  override def visitYearMonthIntervalDataType(ctx: YearMonthIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = YearMonthIntervalType.stringToField(startStr)
    if (ctx.to != null) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = YearMonthIntervalType.stringToField(endStr)
      if (end <= start) {
        throw QueryParsingErrors.fromToIntervalUnsupportedError(startStr, endStr, ctx)
      }
      YearMonthIntervalType(start, end)
    } else {
      YearMonthIntervalType(start)
    }
  }

  override def visitDayTimeIntervalDataType(ctx: DayTimeIntervalDataTypeContext): DataType = {
    val startStr = ctx.from.getText.toLowerCase(Locale.ROOT)
    val start = DayTimeIntervalType.stringToField(startStr)
    if (ctx.to != null ) {
      val endStr = ctx.to.getText.toLowerCase(Locale.ROOT)
      val end = DayTimeIntervalType.stringToField(endStr)
      if (end <= start) {
        throw QueryParsingErrors.fromToIntervalUnsupportedError(startStr, endStr, ctx)
      }
      DayTimeIntervalType(start, end)
    } else {
      DayTimeIntervalType(start)
    }
  }

  /**
   * Create a complex DataType. Arrays, Maps and Structures are supported.
   */
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case SqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case SqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case SqlBaseParser.STRUCT =>
        StructType(Option(ctx.complexColTypeList).toArray.flatMap(visitComplexColTypeList))
    }
  }

  /**
   * Create top level table schema.
   */
  protected def createSchema(ctx: ColTypeListContext): StructType = {
    StructType(Option(ctx).toArray.flatMap(visitColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType).toSeq
  }

  /**
   * Create a top level [[StructField]] from a column definition.
   */
  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._

    val builder = new MetadataBuilder
    // Add comment to metadata
    Option(commentSpec()).map(visitCommentSpec).foreach {
      builder.putString("comment", _)
    }

    StructField(
      name = colName.getText,
      dataType = typedVisit[DataType](ctx.dataType),
      nullable = NULL == null,
      metadata = builder.build())
  }

  /**
   * Create a [[StructType]] from a sequence of [[StructField]]s.
   */
  protected def createStructType(ctx: ComplexColTypeListContext): StructType = {
    StructType(Option(ctx).toArray.flatMap(visitComplexColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitComplexColTypeList(ctx: ComplexColTypeListContext): Seq[StructField] = {
    withOrigin(ctx) {
      ctx.complexColType().asScala.map(visitComplexColType).toSeq
    }
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitComplexColType(ctx: ComplexColTypeContext): StructField = withOrigin(ctx) {
    import ctx._
    val structField = StructField(
      name = identifier.getText,
      dataType = typedVisit(dataType()),
      nullable = NULL == null)
    Option(commentSpec).map(visitCommentSpec).map(structField.withComment).getOrElse(structField)
  }

  /**
   * Create a comment string.
   */
  override def visitCommentSpec(ctx: CommentSpecContext): String = withOrigin(ctx) {
    string(visitStringLit(ctx.stringLit))
  }

  /**
   * Returns a collation name.
   */
  override def visitCollateClause(ctx: CollateClauseContext): String = withOrigin(ctx) {
    ctx.identifier.getText
  }
}
