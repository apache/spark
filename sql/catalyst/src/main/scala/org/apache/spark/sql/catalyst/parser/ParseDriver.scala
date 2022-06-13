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

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.TerminalNodeImpl

import org.apache.spark.SparkThrowableHelper
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLConfHelper, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserUtils.withOrigin
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.errors.QueryParsingErrors
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Base SQL parsing infrastructure.
 */
abstract class AbstractSqlParser extends ParserInterface with SQLConfHelper with Logging {

  /** Creates/Resolves DataType for a given SQL string. */
  override def parseDataType(sqlText: String): DataType = parse(sqlText) { parser =>
    astBuilder.visitSingleDataType(parser.singleDataType())
  }

  /** Creates Expression for a given SQL string. */
  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    val ctx = parser.singleExpression()
    withOrigin(ctx, Some(sqlText)) {
      astBuilder.visitSingleExpression(ctx)
    }
  }

  /** Creates TableIdentifier for a given SQL string. */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText) { parser =>
    astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier())
  }

  /** Creates FunctionIdentifier for a given SQL string. */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    parse(sqlText) { parser =>
      astBuilder.visitSingleFunctionIdentifier(parser.singleFunctionIdentifier())
    }
  }

  /** Creates a multi-part identifier for a given SQL string */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    parse(sqlText) { parser =>
      astBuilder.visitSingleMultipartIdentifier(parser.singleMultipartIdentifier())
    }
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = parse(sqlText) { parser =>
    astBuilder.visitSingleTableSchema(parser.singleTableSchema())
  }

  /** Creates LogicalPlan for a given SQL string of query. */
  override def parseQuery(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val ctx = parser.query()
    withOrigin(ctx, Some(sqlText)) {
      astBuilder.visitQuery(ctx)
    }
  }

  /** Creates LogicalPlan for a given SQL string. */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    val ctx = parser.singleStatement()
    withOrigin(ctx, Some(sqlText)) {
      astBuilder.visitSingleStatement(ctx) match {
        case plan: LogicalPlan => plan
        case _ =>
          val position = Origin(None, None)
          throw QueryParsingErrors.sqlStatementUnsupportedError(sqlText, position)
      }
    }
  }

  /** Get the builder (visitor) which converts a ParseTree into an AST. */
  protected def astBuilder: AstBuilder

  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logDebug(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.addParseListener(UnclosedCommentProcessor(command, tokenStream))
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.setErrorHandler(new SparkParserErrorStrategy())
    parser.legacy_setops_precedence_enabled = conf.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = conf.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = conf.enforceReservedKeywords

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position,
          e.errorClass, None, e.messageParameters)
    }
  }
}

/**
 * Concrete SQL parser for Catalyst-only SQL statements.
 */
class CatalystSqlParser extends AbstractSqlParser {
  val astBuilder = new AstBuilder
}

/** For test-only. */
object CatalystSqlParser extends CatalystSqlParser

/**
 * This string stream provides the lexer with upper case characters only. This greatly simplifies
 * lexing the stream, while we can maintain the original command.
 *
 * This is based on Hive's org.apache.hadoop.hive.ql.parse.ParseDriver.ANTLRNoCaseStringStream
 *
 * The comment below (taken from the original class) describes the rationale for doing this:
 *
 * This class provides and implementation for a case insensitive token checker for the lexical
 * analysis part of antlr. By converting the token stream into upper case at the time when lexical
 * rules are checked, this class ensures that the lexical rules need to just match the token with
 * upper case letters as opposed to combination of upper case and lower case characters. This is
 * purely used for matching lexical rules. The actual token text is stored in the same way as the
 * user input without actually converting it into an upper case. The token values are generated by
 * the consume() function of the super class ANTLRStringStream. The LA() function is the lookahead
 * function and is purely used for matching lexical rules. This also means that the grammar will
 * only accept capitalized tokens in case it is run from other tools like antlrworks which do not
 * have the UpperCaseCharStream implementation.
 */

private[parser] class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
}

/**
 * The ParseErrorListener converts parse errors into AnalysisExceptions.
 */
case object ParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    e match {
      case sre: SparkRecognitionException if sre.errorClass.isDefined =>
        throw new ParseException(None, start, stop, sre.errorClass.get, sre.messageParameters)
      case _ =>
        throw new ParseException(None, msg, start, stop)
    }
  }
}

/**
 * A [[ParseException]] is an [[AnalysisException]] that is thrown during the parse process. It
 * contains fields and an extended error message that make reporting and diagnosing errors easier.
 */
class ParseException(
    val command: Option[String],
    message: String,
    val start: Origin,
    val stop: Origin,
    errorClass: Option[String] = None,
    errorSubClass: Option[String] = None,
    messageParameters: Array[String] = Array.empty)
  extends AnalysisException(
    message,
    start.line,
    start.startPosition,
    None,
    None,
    errorClass,
    errorSubClass,
    messageParameters) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(Option(ParserUtils.command(ctx)),
      message,
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop))
  }

  def this(errorClass: String, messageParameters: Array[String], ctx: ParserRuleContext) =
    this(Option(ParserUtils.command(ctx)),
      SparkThrowableHelper.getMessage(errorClass, null, messageParameters),
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop),
      Some(errorClass),
      None,
      messageParameters)

  def this(errorClass: String,
           errorSubClass: String,
           messageParameters: Array[String],
           ctx: ParserRuleContext) =
    this(Option(ParserUtils.command(ctx)),
      SparkThrowableHelper.getMessage(errorClass, errorSubClass, messageParameters),
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop),
      Some(errorClass),
      Some(errorSubClass),
      messageParameters)

  /** Compose the message through SparkThrowableHelper given errorClass and messageParameters. */
  def this(
      command: Option[String],
      start: Origin,
      stop: Origin,
      errorClass: String,
      messageParameters: Array[String]) =
    this(
      command,
      SparkThrowableHelper.getMessage(errorClass, null, messageParameters),
      start,
      stop,
      Some(errorClass),
      None,
      messageParameters)

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p), _, _, _, _, _) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): ParseException = {
    // PARSE_EMPTY_STATEMENT error class overrides the PARSE_SYNTAX_ERROR when cmd is empty
    if (cmd.trim().isEmpty && errorClass.isDefined && errorClass.get == "PARSE_SYNTAX_ERROR") {
      new ParseException(Option(cmd), start, stop, "PARSE_EMPTY_STATEMENT", Array[String]())
    } else {
      new ParseException(Option(cmd), message, start, stop, errorClass, None, messageParameters)
    }
  }
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
case object PostProcessor extends SqlBaseParserBaseListener {

  /** Throws error message when exiting a explicitly captured wrong identifier rule */
  override def exitErrorIdent(ctx: SqlBaseParser.ErrorIdentContext): Unit = {
    val ident = ctx.getParent.getText

    throw QueryParsingErrors.unquotedIdentifierError(ident, ctx)
  }

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: SqlBaseParser.QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: SqlBaseParser.NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
      ctx: ParserRuleContext,
      stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      SqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}

/**
 * The post-processor checks the unclosed bracketed comment.
 */
case class UnclosedCommentProcessor(
    command: String, tokenStream: CommonTokenStream) extends SqlBaseParserBaseListener {

  override def exitSingleDataType(ctx: SqlBaseParser.SingleDataTypeContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleExpression(ctx: SqlBaseParser.SingleExpressionContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleTableIdentifier(ctx: SqlBaseParser.SingleTableIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleFunctionIdentifier(
      ctx: SqlBaseParser.SingleFunctionIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleMultipartIdentifier(
      ctx: SqlBaseParser.SingleMultipartIdentifierContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleTableSchema(ctx: SqlBaseParser.SingleTableSchemaContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitQuery(ctx: SqlBaseParser.QueryContext): Unit = {
    checkUnclosedComment(tokenStream, command)
  }

  override def exitSingleStatement(ctx: SqlBaseParser.SingleStatementContext): Unit = {
    // SET command uses a wildcard to match anything, and we shouldn't parse the comments, e.g.
    // `SET myPath =/a/*`.
    if (!ctx.statement().isInstanceOf[SqlBaseParser.SetConfigurationContext]) {
      checkUnclosedComment(tokenStream, command)
    }
  }

  /** check `has_unclosed_bracketed_comment` to find out the unclosed bracketed comment. */
  private def checkUnclosedComment(tokenStream: CommonTokenStream, command: String) = {
    assert(tokenStream.getTokenSource.isInstanceOf[SqlBaseLexer])
    val lexer = tokenStream.getTokenSource.asInstanceOf[SqlBaseLexer]
    if (lexer.has_unclosed_bracketed_comment) {
      // The last token is 'EOF' and the penultimate is unclosed bracketed comment
      val failedToken = tokenStream.get(tokenStream.size() - 2)
      assert(failedToken.getType() == SqlBaseParser.BRACKETED_COMMENT)
      val position = Origin(Option(failedToken.getLine), Option(failedToken.getCharPositionInLine))
      throw QueryParsingErrors.unclosedBracketedCommentError(command, position)
    }
  }

}
