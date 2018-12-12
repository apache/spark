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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Base SQL parsing infrastructure.
 */
abstract class AbstractSqlParser extends ParserInterface with Logging {

  /** Creates/Resolves DataType for a given SQL string. */
  override def parseDataType(sqlText: String): DataType = parse(sqlText) { parser =>
    astBuilder.visitSingleDataType(parser.singleDataType())
  }

  /** Creates Expression for a given SQL string. */
  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    astBuilder.visitSingleExpression(parser.singleExpression())
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

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = parse(sqlText) { parser =>
    astBuilder.visitSingleTableSchema(parser.singleTableSchema())
  }

  /** Creates LogicalPlan for a given SQL string. */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ =>
        val position = Origin(None, None)
        throw new ParseException(Option(sqlText), "Unsupported SQL statement", position, position)
    }
  }

  /** Get the builder (visitor) which converts a ParseTree into an AST. */
  protected def astBuilder: AstBuilder

  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logDebug(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)
    lexer.legacy_setops_precedence_enbled = SQLConf.get.setOpsPrecedenceEnforced

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)
    parser.legacy_setops_precedence_enbled = SQLConf.get.setOpsPrecedenceEnforced

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
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
}

/**
 * Concrete SQL parser for Catalyst-only SQL statements.
 */
class CatalystSqlParser(conf: SQLConf) extends AbstractSqlParser {
  val astBuilder = new AstBuilder(conf)
}

/** For test-only. */
object CatalystSqlParser extends AbstractSqlParser {
  val astBuilder = new AstBuilder(new SQLConf())
}

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

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

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
    val position = Origin(Some(line), Some(charPositionInLine))
    throw new ParseException(None, msg, position, position)
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
    val stop: Origin) extends AnalysisException(message, start.line, start.startPosition) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(Option(ParserUtils.command(ctx)),
      message,
      ParserUtils.position(ctx.getStart),
      ParserUtils.position(ctx.getStop))
  }

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p)) =>
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
    new ParseException(Option(cmd), message, start, stop)
  }
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
case object PostProcessor extends SqlBaseBaseListener {

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
