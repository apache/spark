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

import org.antlr.v4.runtime.{CharStream, CommonToken, Token, TokenSource}
import org.antlr.v4.runtime.misc.Pair
import org.antlr.v4.runtime.tree.ParseTree

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.catalyst.util.SparkParserUtils.{string, withOrigin}
import org.apache.spark.sql.connector.catalog.IdentityColumnSpec
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, CalendarIntervalType, CharType, DataType, DateType, DayTimeIntervalType, DecimalType, DoubleType, FloatType, GeographyType, GeometryType, IntegerType, LongType, MapType, MetadataBuilder, NullType, ShortType, StringType, StructField, StructType, TimestampNTZType, TimestampType, TimeType, VarcharType, VariantType, YearMonthIntervalType}

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

  // Visit stringLit which can now have multiple stringLitWithoutMarker or parameterMarker children
  override def visitStringLit(ctx: StringLitContext): Token = {
    if (ctx == null) {
      return null
    }

    // Collect all children (could be mix of stringLitWithoutMarker and parameterMarker)
    val children = scala.collection.mutable.ListBuffer[Token]()
    // Visit each child and collect resulting tokens
    val childCount = ctx.getChildCount
    var i = 0
    while (i < childCount) {
      val child = ctx.getChild(i)
      val token = visit(child).asInstanceOf[Token]
      if (token != null) {
        children += token
      }
      i += 1
    }

    if (children.isEmpty) {
      null
    } else if (children.size == 1) {
      // Fast path: single token, return unchanged
      children.head
    } else {
      // Multiple tokens: create coalesced token
      createCoalescedStringToken(children.toSeq)
    }
  }

  // Visit stringLitWithoutMarker which now supports coalescing with +
  override def visitStringLitWithoutMarker(ctx: StringLitWithoutMarkerContext): Token = {
    if (ctx == null) {
      return null
    }

    // Collect all string literal terminals (could be multiple with stringLitWithoutMarker+)
    val allTerminals = collectStringTerminals(ctx)

    if (allTerminals.isEmpty) {
      null
    } else if (allTerminals.size == 1) {
      // Fast path: single literal, return original token unchanged
      allTerminals.head.getSymbol
    } else {
      // Multiple literals: create coalesced token
      createCoalescedStringToken(allTerminals.map(_.getSymbol).toSeq)
    }
  }

  /**
   * Visit a singleStringLit rule and return the token (always exactly one token).
   */
  override def visitSingleStringLit(ctx: SingleStringLitContext): Token = {
    if (ctx == null) {
      return null
    }

    if (ctx.STRING_LITERAL() != null) {
      ctx.STRING_LITERAL().getSymbol
    } else {
      ctx.DOUBLEQUOTED_STRING().getSymbol
    }
  }

  /**
   * Collect all STRING_LITERAL and DOUBLEQUOTED_STRING terminals from the context. With the
   * stringLitWithoutMarker+ grammar rule, there can be multiple terminals.
   */
  private def collectStringTerminals(
      ctx: StringLitWithoutMarkerContext): Seq[org.antlr.v4.runtime.tree.TerminalNode] = {
    val stringLiterals = if (ctx.STRING_LITERAL != null) {
      // With stringLit+, ANTLR generates a List
      ctx.STRING_LITERAL
        .asInstanceOf[java.util.List[_]]
        .asScala
        .map(_.asInstanceOf[org.antlr.v4.runtime.tree.TerminalNode])
        .toSeq
    } else {
      Seq.empty
    }

    val doubleQuoted = if (ctx.DOUBLEQUOTED_STRING != null) {
      // With stringLit+, ANTLR generates a List
      ctx.DOUBLEQUOTED_STRING
        .asInstanceOf[java.util.List[_]]
        .asScala
        .map(_.asInstanceOf[org.antlr.v4.runtime.tree.TerminalNode])
        .toSeq
    } else {
      Seq.empty
    }

    // Combine and sort by position in source
    (stringLiterals ++ doubleQuoted).sortBy(_.getSymbol.getStartIndex)
  }

  /**
   * Create a CoalescedStringToken from multiple string literal tokens. Processes each token
   * through string() to handle escaping, then concatenates.
   */
  private def createCoalescedStringToken(tokens: Seq[Token]): Token = {
    val firstToken = tokens.head
    val lastToken = tokens.last

    // Check if any of the tokens are R-strings
    val hasRString = tokens.exists { token =>
      val text = token.getText
      text.length >= 2 &&
      (text.charAt(0) == 'R' || text.charAt(0) == 'r') &&
      (text.charAt(1) == '\'' || text.charAt(1) == '"')
    }

    // Determine the quote character to use (preserve from first non-R-string token)
    val quoteChar = {
      val firstNonRToken = tokens
        .find { token =>
          val text = token.getText
          !(text.length >= 2 &&
            (text.charAt(0) == 'R' || text.charAt(0) == 'r') &&
            (text.charAt(1) == '\'' || text.charAt(1) == '"'))
        }
        .getOrElse(tokens.head)

      val text = firstNonRToken.getText
      if (text.startsWith("\"") || (text.length >= 2 && text.charAt(1) == '"')) {
        '"'
      } else {
        '\''
      }
    }

    // Concatenate the raw content of each token (without the outer quotes).
    // Preserve all inner content including "" or '' sequences - these will be
    // handled later by unescapeSQLString based on the config.
    val coalescedRawContent = tokens.map { token =>
      val text = token.getText
      // Check if this is an R-string (raw string literal)
      val isRString = text.length >= 2 &&
        (text.charAt(0) == 'R' || text.charAt(0) == 'r') &&
        (text.charAt(1) == '\'' || text.charAt(1) == '"')

      if (isRString) {
        // For R-strings: Remove R prefix and outer quotes (first 2 chars and last char)
        text.substring(2, text.length - 1)
      } else {
        // For regular strings: Remove only the outer quotes (first and last character)
        // Keep all inner content including "" or '' which will be processed by unescapeSQLString
        text.substring(1, text.length - 1)
      }
    }.mkString

    new CoalescedStringToken(
      new org.antlr.v4.runtime.misc.Pair(firstToken.getTokenSource, firstToken.getInputStream),
      firstToken.getType,
      firstToken.getChannel,
      firstToken.getStartIndex,
      lastToken.getStopIndex,
      coalescedRawContent,
      hasRString,
      quoteChar)
  }

  override def visitNamedParameterMarkerRule(ctx: NamedParameterMarkerRuleContext): Token = {
    // This should be unreachable due to grammar-level blocking of parameter markers
    // when legacy parameter substitution is enabled
    QueryParsingErrors.unexpectedUseOfParameterMarker(ctx)
  }

  override def visitPositionalParameterMarkerRule(
      ctx: PositionalParameterMarkerRuleContext): Token = {
    // This should be unreachable due to grammar-level blocking of parameter markers
    // when legacy parameter substitution is enabled
    QueryParsingErrors.unexpectedUseOfParameterMarker(ctx)
  }

  override def visitNamedParameterLiteral(ctx: NamedParameterLiteralContext): AnyRef = {
    // Parameter markers are not allowed in data type definitions
    QueryParsingErrors.unexpectedUseOfParameterMarker(ctx)
  }

  override def visitPosParameterLiteral(ctx: PosParameterLiteralContext): AnyRef = {
    // Parameter markers are not allowed in data type definitions
    QueryParsingErrors.unexpectedUseOfParameterMarker(ctx)
  }

  /**
   * Gets the integer value from an IntegerValueContext after parameter replacement. Asserts that
   * parameter markers have been substituted before reaching DataTypeAstBuilder.
   *
   * @param ctx
   *   The IntegerValueContext to extract the integer from
   * @return
   *   The integer value
   */
  protected def getIntegerValue(ctx: IntegerValueContext): Int = {
    assert(
      !ctx.isInstanceOf[ParameterIntegerValueContext],
      "Parameter markers should be substituted before DataTypeAstBuilder processes the " +
        s"parse tree. Found unsubstituted parameter: ${ctx.getText}")
    ctx.getText.toInt
  }

  /**
   * Create a multi-part identifier.
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] =
    withOrigin(ctx) {
      ctx.parts.asScala.map(_.getText).toSeq
    }

  /**
   * Resolve/create a primitive type.
   */
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    val typeCtx = ctx.primitiveType
    if (typeCtx.nonTrivialPrimitiveType != null) {
      // This is a primitive type with parameters, e.g. VARCHAR(10), DECIMAL(10, 2), etc.
      val currentCtx = typeCtx.nonTrivialPrimitiveType
      currentCtx.start.getType match {
        case STRING =>
          currentCtx.children.asScala.toSeq match {
            case Seq(_) => StringType
            case Seq(_, ctx: CollateClauseContext) =>
              val collationNameParts = visitCollateClause(ctx).toArray
              val collationId = CollationFactory.collationNameToId(
                CollationFactory.resolveFullyQualifiedName(collationNameParts))
              StringType(collationId)
          }
        case CHARACTER | CHAR =>
          if (currentCtx.length == null) {
            throw QueryParsingErrors.charVarcharTypeMissingLengthError(typeCtx.getText, ctx)
          } else CharType(currentCtx.length.getText.toInt)
        case VARCHAR =>
          if (currentCtx.length == null) {
            throw QueryParsingErrors.charVarcharTypeMissingLengthError(typeCtx.getText, ctx)
          } else VarcharType(currentCtx.length.getText.toInt)
        case DECIMAL | DEC | NUMERIC =>
          if (currentCtx.precision == null) {
            DecimalType.USER_DEFAULT
          } else if (currentCtx.scale == null) {
            DecimalType(currentCtx.precision.getText.toInt, 0)
          } else {
            DecimalType(currentCtx.precision.getText.toInt, currentCtx.scale.getText.toInt)
          }
        case INTERVAL =>
          if (currentCtx.fromDayTime != null) {
            visitDayTimeIntervalDataType(currentCtx)
          } else if (currentCtx.fromYearMonth != null) {
            visitYearMonthIntervalDataType(currentCtx)
          } else {
            CalendarIntervalType
          }
        case TIMESTAMP =>
          if (currentCtx.WITHOUT() == null) {
            SqlApiConf.get.timestampType
          } else TimestampNTZType
        case TIME =>
          val precision = if (currentCtx.precision == null) {
            TimeType.DEFAULT_PRECISION
          } else {
            currentCtx.precision.getText.toInt
          }
          TimeType(precision)
        case GEOGRAPHY =>
          // Unparameterized geometry type isn't supported and will be caught by the default branch.
          // Here, we only handle the parameterized GEOGRAPHY type syntax, which comes in two forms:
          if (currentCtx.any != null) {
            // The special parameterized GEOGRAPHY type syntax uses a single "ANY" string value.
            // This implies a mixed GEOGRAPHY type, with potentially different SRIDs across rows.
            GeographyType("ANY")
          } else {
            // The explicitly parameterzied GEOGRAPHY syntax uses a specified integer SRID value.
            // This implies a fixed GEOGRAPHY type, with a single fixed SRID value across all rows.
            GeographyType(currentCtx.srid.getText.toInt)
          }
        case GEOMETRY =>
          // Unparameterized geometry type isn't supported and will be caught by the default branch.
          // Here, we only handle the parameterized GEOMETRY type syntax, which comes in two forms:
          if (currentCtx.any != null) {
            // The special parameterized GEOMETRY type syntax uses a single "ANY" string value.
            // This implies a mixed GEOMETRY type, with potentially different SRIDs across rows.
            GeometryType("ANY")
          } else {
            // The explicitly parameterzied GEOMETRY type syntax has a single integer SRID value.
            // This implies a fixed GEOMETRY type, with a single fixed SRID value across all rows.
            GeometryType(currentCtx.srid.getText.toInt)
          }
      }
    } else if (typeCtx.trivialPrimitiveType != null) {
      // This is a primitive type without parameters, e.g. BOOLEAN, TINYINT, etc.
      typeCtx.trivialPrimitiveType.start.getType match {
        case BOOLEAN => BooleanType
        case TINYINT | BYTE => ByteType
        case SMALLINT | SHORT => ShortType
        case INT | INTEGER => IntegerType
        case BIGINT | LONG => LongType
        case FLOAT | REAL => FloatType
        case DOUBLE => DoubleType
        case DATE => DateType
        case TIMESTAMP_LTZ => TimestampType
        case TIMESTAMP_NTZ => TimestampNTZType
        case BINARY => BinaryType
        case VOID => NullType
        case VARIANT => VariantType
      }
    } else {
      val badType = typeCtx.unsupportedType.getText
      val params = typeCtx
        .integerValue()
        .asScala
        .map(getIntegerValue(_).toString)
        .toList
      val dtStr =
        if (params.nonEmpty) s"$badType(${params.mkString(",")})"
        else badType
      throw QueryParsingErrors.dataTypeUnsupportedError(dtStr, ctx)
    }
  }

  private def visitYearMonthIntervalDataType(ctx: NonTrivialPrimitiveTypeContext): DataType = {
    val startStr = ctx.fromYearMonth.getText.toLowerCase(Locale.ROOT)
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

  private def visitDayTimeIntervalDataType(ctx: NonTrivialPrimitiveTypeContext): DataType = {
    val startStr = ctx.fromDayTime.getText.toLowerCase(Locale.ROOT)
    val start = DayTimeIntervalType.stringToField(startStr)
    if (ctx.to != null) {
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
    if (ctx.LT() == null && ctx.NEQ() == null) {
      throw QueryParsingErrors.nestedTypeMissingElementTypeError(ctx.getText, ctx)
    }
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
      name = errorCapturingIdentifier.getText,
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
  override def visitCollateClause(ctx: CollateClauseContext): Seq[String] = withOrigin(ctx) {
    visitMultipartIdentifier(ctx.collationName)
  }

  /**
   * Parse and verify IDENTITY column definition.
   *
   * @param ctx
   *   The parser context.
   * @param dataType
   *   The data type of column defined as IDENTITY column. Used for verification.
   * @return
   *   Tuple containing start, step and allowExplicitInsert.
   */
  protected def visitIdentityColumn(
      ctx: IdentityColumnContext,
      dataType: DataType): IdentityColumnSpec = {
    if (dataType != LongType && dataType != IntegerType) {
      throw QueryParsingErrors.identityColumnUnsupportedDataType(ctx, dataType.toString)
    }
    // We support two flavors of syntax:
    // (1) GENERATED ALWAYS AS IDENTITY (...)
    // (2) GENERATED BY DEFAULT AS IDENTITY (...)
    // (1) forbids explicit inserts, while (2) allows.
    val allowExplicitInsert = ctx.BY() != null && ctx.DEFAULT() != null
    val (start, step) = visitIdentityColSpec(ctx.identityColSpec())

    new IdentityColumnSpec(start, step, allowExplicitInsert)
  }

  override def visitIdentityColSpec(ctx: IdentityColSpecContext): (Long, Long) = {
    val defaultStart = 1
    val defaultStep = 1
    if (ctx == null) {
      return (defaultStart, defaultStep)
    }
    var (start, step): (Option[Long], Option[Long]) = (None, None)
    ctx.sequenceGeneratorOption().asScala.foreach { option =>
      if (option.start != null) {
        if (start.isDefined) {
          throw QueryParsingErrors.identityColumnDuplicatedSequenceGeneratorOption(ctx, "START")
        }
        start = Some(option.start.getText.toLong)
      } else if (option.step != null) {
        if (step.isDefined) {
          throw QueryParsingErrors.identityColumnDuplicatedSequenceGeneratorOption(ctx, "STEP")
        }
        step = Some(option.step.getText.toLong)
        if (step.get == 0L) {
          throw QueryParsingErrors.identityColumnIllegalStep(ctx)
        }
      } else {
        throw SparkException
          .internalError(s"Invalid identity column sequence generator option: ${option.getText}")
      }
    }
    (start.getOrElse(defaultStart), step.getOrElse(defaultStep))
  }
}

/**
 * A synthetic token representing multiple coalesced string literals.
 *
 * When the parser encounters consecutive string literals (e.g., 'hello' 'world'), they are
 * automatically coalesced into a single logical string. This token class represents such
 * coalesced strings while maintaining the Token interface.
 *
 * The coalescedValue contains the raw concatenated content from all the string literals (with
 * outer quotes removed but escape sequences preserved). The getText() method wraps this in
 * quotes, and when SparkParserUtils.string() is called, it will unescape the content based on the
 * current SQL configuration (respecting ESCAPED_STRING_LITERALS).
 *
 * @param source
 *   The token source and input stream
 * @param tokenType
 *   The ANTLR token type (typically STRING_LITERAL)
 * @param channel
 *   The token channel
 * @param start
 *   The start index of the first literal in the input stream
 * @param stop
 *   The stop index of the last literal in the input stream
 * @param coalescedValue
 *   The raw concatenated content (without outer quotes, escape sequences NOT processed)
 */
private[parser] class CoalescedStringToken(
    source: Pair[TokenSource, CharStream],
    tokenType: Int,
    channel: Int,
    start: Int,
    stop: Int,
    private val coalescedValue: String,
    private val isRawString: Boolean = false,
    private val quoteChar: Char = '\'')
    extends CommonToken(source, tokenType, channel, start, stop) {

  override def getText: String = {
    if (isRawString) {
      // Preserve R-string prefix so unescapeSQLString knows not to process escapes
      s"R$quoteChar$coalescedValue$quoteChar"
    } else {
      s"$quoteChar$coalescedValue$quoteChar"
    }
  }

  override def toString: String = {
    if (isRawString) {
      s"CoalescedStringToken(R$quoteChar$coalescedValue$quoteChar)"
    } else {
      s"CoalescedStringToken($quoteChar$coalescedValue$quoteChar)"
    }
  }
}
