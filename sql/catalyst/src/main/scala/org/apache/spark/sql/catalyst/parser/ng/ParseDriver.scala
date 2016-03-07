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
package org.apache.spark.sql.catalyst.parser.ng

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}

import org.apache.spark.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ng.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.Utils

/**
 * Base SQL parsing infrastructure.
 */
abstract class AbstractSqlParser extends ParserInterface with Logging {

  /** Creates Expression for a given SQL string. */
  override def parseExpression(sqlText: String): Expression = parse(sqlText) { parser =>
    astBuilder.visitSingleExpression(parser.singleExpression())
  }

  /** Creates TableIdentifier for a given SQL string. */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = parse(sqlText) { parser =>
    astBuilder.visitSingleTableIdentifier(parser.singleTableIdentifier())
  }

  /** Creates LogicalPlan for a given SQL string. */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    astBuilder.visitSingleStatement(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ => nativeCommand(sqlText)
    }
  }

  /** Get the builder (visitor) which converts a ParseTree into a AST. */
  protected def astBuilder: AstBuilder

  /** Create a native command, or fail when this is not supported. */
  protected def nativeCommand(sqlText: String): LogicalPlan = {
    throw new AnalysisException(s"Unsupported SQL statement:\n$sqlText")
  }

  protected def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    logInfo(s"Parsing command: $command")

    val lexer = new SqlBaseLexer(new ANTLRNoCaseStringStream(command))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new SqlBaseParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case e: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.reset() // rewind input stream
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
        (e.line, e.startPosition) match {
          case (Some(line), Some(position)) =>
            throw new ParseException(
              Option(command),
              e.message,
              (line, position),
              (line, position))
          case _ => throw e
        }
    }
  }
}

/**
 * Concrete SQL parser for Catalyst-only SQL statements.
 */
object CatalystSqlParser extends AbstractSqlParser {
  val astBuilder = new AstBuilder
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
 * have the ANTLRNoCaseStringStream implementation.
 */

private[parser] class ANTLRNoCaseStringStream(input: String) extends ANTLRInputStream(input) {
  override def LA(i: Int): Int = {
    val la = super.LA(i)
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
    val position = (line, charPositionInLine)
    throw new ParseException(None, msg, position, position)
  }
}

/**
 * A [[ParseException]] is an [[AnalysisException]] that is thrown during the parse process. It
 * contains a fields and an extended error message that make reporting and diagnosing errors easier.
 */
class ParseException(
    val command: Option[String],
    message: String,
    val start: (Int, Int),
    val stop: (Int, Int)) extends AnalysisException(message, Some(start._1), Some(start._2)) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(ParseException.cmd(ctx),
      message,
      ParseException.position(ctx.getStart),
      ParseException.position(ctx.getStop))
  }

  override def getMessage: String = {
    val (line, position) = start

    // Build the error message.
    val location = s"(line $line, pos $position)"
    val builder = new StringBuilder
    builder ++= s"\n$message $location\n"
    command.foreach { cmd =>
      val (above, below) = cmd.split("\n").splitAt(line)
      builder ++= "\n== SQL ==\n"
      above.foreach(builder ++= _ += '\n')
      builder ++= (0 until position).map(_ => "-").mkString("") ++= "^^^\n"
      below.foreach(builder ++= _ += '\n')
    }
    builder.toString
  }

  def withCommand(cmd: String): ParseException = {
    new ParseException(Option(cmd), message, start, stop)
  }
}

object ParseException {
  /** Get the command which created the token. */
  def cmd(ctx: ParserRuleContext): Option[String] = {
    cmd(ctx.getStart.getInputStream)
  }

  /** Get the command which created the token. */
  def cmd(stream: CharStream): Option[String] = {
    Option(stream.getText(Interval.of(0, stream.size())))
  }

  /** Get the line and position of the token. */
  def position(token: Token): (Int, Int) = (token.getLine, token.getCharPositionInLine)
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
case object PostProcessor extends SqlBaseBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
      ctx: ParserRuleContext,
      stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    parent.addChild(f(new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      SqlBaseParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)))
  }
}
