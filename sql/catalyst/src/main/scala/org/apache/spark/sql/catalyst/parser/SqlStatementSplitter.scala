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

import scala.collection.mutable

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.ParseCancellationException

import org.apache.spark.sql.internal.SqlApiConf

/**
 * Represents a single complete SQL statement together with the delimiter that
 * terminated it (always `";"` for now).
 *
 * @param statement   the SQL statement text, with surrounding whitespace trimmed and
 *                    without the terminator
 * @param terminator  the delimiter string that terminated the statement
 */
case class SqlStatement(statement: String, terminator: String) {
  override def toString: String = statement + terminator
}

/**
 * Result of splitting a SQL string into individual statements.
 *
 * @param completeStatements  statements that are fully terminated by `;`
 * @param partialStatement    trailing text after the last `;` that has not yet
 *                            formed a complete statement; an empty string when the
 *                            input ends with `;` or contains no significant
 *                            trailing content
 * @param hasUnclosedComment  true when [[partialStatement]] contains an unclosed
 *                            bracketed comment (`/* ...` with no matching `*/`).
 *                            Interactive CLIs may want to flush the partial to the
 *                            backend in this case (so the user sees a parse error)
 *                            rather than keep buffering, since the input cannot be
 *                            completed simply by appending more SQL.
 */
case class SqlStatementSplitResult(
    completeStatements: Seq[SqlStatement],
    partialStatement: String,
    hasUnclosedComment: Boolean = false) {
  def isEmpty: Boolean = completeStatements.isEmpty && partialStatement.isEmpty
}

/**
 * A parser-based SQL statement splitter, inspired by Trino's
 * `io.trino.cli.lexer.StatementSplitter`.
 *
 * Each candidate statement is consumed and confirmed by the ANTLR-generated
 * [[SqlBaseParser]] via the existing `compoundOrSingleStatement` rule (the same
 * rule that the normal Spark SQL parser uses). The splitter:
 *
 *   1. Tokenizes the input once.
 *   2. Walks through the token stream. At each significant position, the
 *      splitter asks the parser whether the prefix ending at the next `;` is a
 *      complete statement; if not, it extends the prefix to the next `;` and
 *      re-tries. This is how SQL scripting `BEGIN ... END` blocks (whose body
 *      contains semicolons) end up emitted as a single statement: only the
 *      prefix that includes a matching `END` is accepted by the parser.
 *   3. When the parser fails because it reached EOF mid-rule (e.g. an
 *      un-terminated `BEGIN ... END`, a SELECT with a missing operand), the
 *      remaining input is treated as a partial statement so an interactive
 *      caller can keep buffering.
 *   4. When the parser fails on a non-EOF token (the input is structurally
 *      invalid), the splitter falls back to splitting at the next `;` so the
 *      surrounding delimiters still emit chunks and the backend can report
 *      the error per chunk.
 *
 * Quoted strings, single-line and bracketed (nested) comments are honored
 * throughout. An unterminated bracketed comment is surfaced via
 * [[SqlStatementSplitResult.hasUnclosedComment]].
 *
 * The optional `validationPreprocess` parameter lets the caller transform the
 * candidate region before the parser sees it -- this exists for the
 * placeholder-substitution hybrid in [[org.apache.spark.sql.execution.SparkSqlParser]]
 * where `${...}` references are replaced with a constant identifier so the
 * parser can confirm block boundaries even when the real variable values are
 * not available yet (they are produced by an earlier `SET` in the same batch).
 * Emission always uses the original (un-preprocessed) token text, so the
 * variable references survive into the emitted statement and are substituted
 * for real at execution time. When `validationPreprocess` is `identity`
 * (the default), the splitter behaves as a pure original-text splitter.
 *
 * Performance note: for a single `BEGIN ... END` block with k internal `;`,
 * the splitter calls `tryParseRegion` O(k) times on growing prefixes -- an
 * O(k^2) cost in the worst case (incomplete block on every keystroke in
 * interactive mode). Ordinary non-scripting SQL is O(n). A non-EOF terminated
 * single-statement rule (read `ctx.getStop` once per region) would make this
 * O(n), but Spark's `setResetStatement` has `SET .*?` / `RESET .*?` wildcards
 * that need an EOF anchor to terminate deterministically, so such a
 * single-statement rule-rewrite does not drop in cleanly. Tracked as a
 * follow-up.
 */
object SqlStatementSplitter {

  /** Split the given SQL text into individual statements at `;` boundaries. */
  def split(sqlText: String): SqlStatementSplitResult =
    split(sqlText, identity)

  /**
   * Split the given SQL text, applying `validationPreprocess` to each candidate
   * region before parser validation. The emitted [[SqlStatement]] text is
   * always the original (un-preprocessed) input; the preprocessor only affects
   * whether the parser accepts a candidate region as a complete statement.
   *
   * Pass `identity` for a pure original-text splitter (the default).
   */
  def split(sqlText: String, validationPreprocess: String => String): SqlStatementSplitResult = {
    require(sqlText != null, "sqlText must not be null")
    require(validationPreprocess != null, "validationPreprocess must not be null")

    val lexer = new SqlBaseLexer(new UpperCaseCharStream(CharStreams.fromString(sqlText)))
    lexer.removeErrorListeners()
    val tokenStream = new CommonTokenStream(lexer)
    tokenStream.fill()

    val numTokens = tokenStream.size()
    // Pre-compute the positions of `;` tokens (on the default channel).
    val delimiterPositions: Array[Int] = {
      val acc = mutable.ArrayBuffer.empty[Int]
      var i = 0
      while (i < numTokens) {
        if (tokenStream.get(i).getType == SqlBaseLexer.SEMICOLON) acc += i
        i += 1
      }
      acc.toArray
    }

    val completeStatements = mutable.ArrayBuffer.empty[SqlStatement]
    val buffer = new StringBuilder()
    // Whether `buffer` contains any non-hidden token (i.e. any actual SQL content
    // beyond whitespace and comments). Chunks that only contain whitespace/comments
    // are dropped, matching the spark-sql CLI's long-standing behavior.
    var bufferHasContent = false
    var index = 0
    var stopOuter = false
    // The first index in `delimiterPositions` that is still > our cursor.
    var delimSearchStart = 0

    // Snapshot the session config once -- the splitter is short-lived and the
    // splitter's parser must agree with the session config on grammar
    // interpretation (e.g. `double_quoted_identifiers`).
    val conf = SqlApiConf.get

    while (!stopOuter && index < numTokens) {
      val startIdx = nextSignificantTokenIndex(tokenStream, index)
      if (startIdx < 0) {
        // Just hidden trailing tokens (whitespace / closed comments). Drain
        // them into the buffer; if any are an unclosed comment, the lexer
        // flag will surface it as a partial.
        while (index < numTokens) {
          val tok = tokenStream.get(index)
          index += 1
          if (tok.getType != Token.EOF) buffer.append(tok.getText)
        }
        stopOuter = true
      } else if (tokenStream.get(startIdx).getType == SqlBaseLexer.SEMICOLON) {
        // The next significant token is itself a `;`. This is an empty
        // statement (e.g. `;;` or leading `;`); drop everything from the
        // cursor through this delimiter and continue.
        index = startIdx + 1
      } else {
        // Advance the search for delimiters past our current cursor.
        while (delimSearchStart < delimiterPositions.length &&
            delimiterPositions(delimSearchStart) <= startIdx) {
          delimSearchStart += 1
        }

        // Try increasingly long prefixes (each ending at a `;`) until the
        // parser accepts one as a complete statement, or we exhaust the
        // delimiters / hit a structural error.
        var parsedOk = false
        var failedNonEof = false
        var matchedDelimIdx = -1
        var d = delimSearchStart
        while (!parsedOk && !failedNonEof && d < delimiterPositions.length) {
          val candidateEnd = delimiterPositions(d)
          tryParseRegion(
            sqlText, tokenStream, startIdx, candidateEnd, validationPreprocess, conf) match {
            case ParsedOk =>
              parsedOk = true
              matchedDelimIdx = candidateEnd
            case FailedAtEof =>
              // Statement body so far is valid but expected more; try a longer
              // prefix that includes the next `;`.
              d += 1
            case FailedNonEof =>
              // Structurally invalid; stop extending.
              failedNonEof = true
          }
        }

        if (parsedOk) {
          // Emit `[startIdx .. matchedDelimIdx)` as one statement, with the
          // delimiter's text as the terminator. Tokens from the cursor up to
          // `startIdx` are leading whitespace/comments that we preserve in the
          // buffer too (so the emitted statement text matches the original
          // input shape, modulo trimming).
          val terminator = tokenStream.get(matchedDelimIdx).getText
          while (index < matchedDelimIdx) {
            val tok = tokenStream.get(index)
            if (tok.getChannel != Token.HIDDEN_CHANNEL) bufferHasContent = true
            buffer.append(tok.getText)
            index += 1
          }
          if (bufferHasContent) {
            val stmt = buffer.toString.trim
            if (stmt.nonEmpty) {
              completeStatements += SqlStatement(stmt, terminator)
            }
          }
          buffer.setLength(0)
          bufferHasContent = false
          index = matchedDelimIdx + 1
          delimSearchStart = d + 1
        } else if (failedNonEof) {
          // Fall back to a single delimiter step so the broken statement is
          // surfaced and the rest can still be split.
          //
          // This is also the path taken for SQL that uses grammar extensions
          // (e.g. Iceberg's `ALTER TABLE ... ADD PARTITION FIELD bucket(...)`):
          // the vanilla parser rejects the prefix, we fall back, and we walk
          // tokens looking for the next `SEMICOLON` token. Because the
          // ANTLR lexer tokenizes `;` inside strings, comments, and
          // back-ticked identifiers as part of the surrounding STRING_LITERAL
          // / SIMPLE_COMMENT / BRACKETED_COMMENT / BACKQUOTED_IDENTIFIER
          // token rather than as a separate SEMICOLON token, those embedded
          // `;` are NOT split-points. A valid `BEGIN ... END`
          // block is always confirmed by the parser (ParsedOk), so this
          // fall-back never splits a well-formed compound block at an
          // internal `;`.
          var stopInner = false
          while (!stopInner && index < numTokens) {
            val token = tokenStream.get(index)
            index += 1
            if (token.getType == Token.EOF) {
              stopInner = true
            } else if (token.getType == SqlBaseLexer.SEMICOLON) {
              if (bufferHasContent) {
                val stmt = buffer.toString.trim
                if (stmt.nonEmpty) {
                  completeStatements += SqlStatement(stmt, token.getText)
                }
              }
              buffer.setLength(0)
              bufferHasContent = false
              stopInner = true
            } else {
              if (token.getChannel != Token.HIDDEN_CHANNEL) bufferHasContent = true
              buffer.append(token.getText)
            }
          }
        } else {
          // All extension attempts failed at EOF (or there were no more
          // delimiters to try). The remainder is a partial statement that the
          // caller may complete by appending more input.
          while (index < numTokens) {
            val tok = tokenStream.get(index)
            index += 1
            if (tok.getType != Token.EOF) {
              if (tok.getChannel != Token.HIDDEN_CHANNEL) bufferHasContent = true
              buffer.append(tok.getText)
            }
          }
          stopOuter = true
        }
      }
    }

    val unclosed = lexer.has_unclosed_bracketed_comment
    val partial =
      if (bufferHasContent || unclosed) buffer.toString.trim else ""
    SqlStatementSplitResult(completeStatements.toSeq, partial, unclosed && partial.nonEmpty)
  }

  /** Outcome of attempting to parse one statement candidate. */
  private sealed trait ParseOutcome
  private case object ParsedOk extends ParseOutcome
  private case object FailedAtEof extends ParseOutcome
  private case object FailedNonEof extends ParseOutcome

  /**
   * Try to parse the region of `sqlText` corresponding to the original token
   * slice `[startIdx, endIdx]` (inclusive on both ends -- `endIdx` is the
   * position of the trailing `;` token whose char range belongs to the
   * region) as a complete top-level Spark SQL statement.
   *
   * The region is extracted from the original source by char-offset
   * (`Token.getStartIndex` / `getStopIndex`), `validationPreprocess` is
   * applied to it, and the result is re-lexed and parsed with a fresh
   * [[SqlBaseParser]]. This isolation means the splitter's parser sees a
   * sub-stream whose EOF lands right after the trailing `;`, so the existing
   * `compoundOrSingleStatement` rule (which requires `SEMICOLON* EOF`) acts
   * as the per-statement validator without any custom grammar rule.
   *
   * Uses the same two-stage SLL -> LL prediction strategy as the main parser
   * for performance (most statements parse cleanly with the faster SLL stage).
   *
   * On failure, we distinguish "the parser still wanted more input" from "the
   * parser saw something it couldn't make sense of" by looking at the
   * remaining input after the bail: if it has been consumed up to EOF, the
   * statement is structurally valid but incomplete -- the caller should try a
   * longer prefix or treat it as partial. Otherwise, the input is invalid and
   * the caller should fall back to delimiter-based splitting.
   *
   * Deeply nested input that throws [[StackOverflowError]] during parsing is
   * treated as [[FailedNonEof]] so the splitter still surfaces the broken
   * input to the backend (which will report the error properly via
   * `QueryParsingErrors.parserStackOverflow`).
   */
  private def tryParseRegion(
      sqlText: String,
      stream: CommonTokenStream,
      startIdx: Int,
      endIdx: Int,
      validationPreprocess: String => String,
      conf: SqlApiConf): ParseOutcome = {
    val firstTok = stream.get(startIdx)
    val lastTok = stream.get(endIdx)
    val regionStart = firstTok.getStartIndex
    // Token.getStopIndex is inclusive, substring's upper bound is exclusive.
    val regionEnd = lastTok.getStopIndex + 1
    val original = sqlText.substring(regionStart, regionEnd)
    val preprocessed = validationPreprocess(original)

    val regionLexer = new SqlBaseLexer(
      new UpperCaseCharStream(CharStreams.fromString(preprocessed)))
    regionLexer.removeErrorListeners()
    val regionTokens = new CommonTokenStream(regionLexer)
    regionTokens.fill()
    val parser = new SqlBaseParser(regionTokens)
    configureSplitterParser(parser, conf)

    // Two-stage parse: SLL first (fast), then LL on failure. Matches the
    // normal parser's `AbstractParser.executeWithTwoStageStrategy`.
    try {
      parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
      parser.compoundOrSingleStatement()
      ParsedOk
    } catch {
      case _: ParseCancellationException =>
        // SLL bailed (could be a false positive). Rewind and retry with LL.
        regionTokens.seek(0)
        parser.reset()
        parser.getInterpreter.setPredictionMode(PredictionMode.LL)
        try {
          parser.compoundOrSingleStatement()
          ParsedOk
        } catch {
          case _: ParseCancellationException =>
            // Use the parser's cursor position (rather than the exception's
            // offending token, which is unreliable under LL adaptive
            // prediction) to tell apart the two failure modes.
            if (regionTokens.LA(1) == Token.EOF) FailedAtEof else FailedNonEof
          case _: StackOverflowError => FailedNonEof
        }
      case _: StackOverflowError => FailedNonEof
    }
  }

  /**
   * Configure a fresh [[SqlBaseParser]] for splitter use: install the managed
   * caches so candidate parses share ANTLR DFA state across calls, apply the
   * session's behavior flags so the splitter agrees with the session parser
   * on grammar interpretation (e.g. `double_quoted_identifiers`), and install
   * a bail error strategy so failures throw immediately.
   *
   * Notably, the splitter does NOT install [[PostProcessor]] or
   * [[UnclosedCommentProcessor]] -- the former mutates the parse tree (which
   * we throw away) and the latter would convert unclosed comments into thrown
   * exceptions, but the splitter surfaces them via the
   * [[SqlStatementSplitResult.hasUnclosedComment]] flag instead.
   */
  private def configureSplitterParser(parser: SqlBaseParser, conf: SqlApiConf): Unit = {
    if (conf.manageParserCaches) AbstractParser.installCaches(parser)

    parser.legacy_setops_precedence_enabled = conf.setOpsPrecedenceEnforced
    parser.legacy_exponent_literal_as_decimal_enabled = conf.exponentLiteralAsDecimalEnabled
    parser.SQL_standard_keyword_behavior = conf.enforceReservedKeywords
    parser.double_quoted_identifiers = conf.doubleQuotedIdentifiers
    parser.parameter_substitution_enabled = !conf.legacyParameterSubstitutionConstantsOnly
    parser.legacy_identifier_clause_only = conf.legacyIdentifierClauseOnly
    parser.single_character_pipe_operator_enabled = conf.singleCharacterPipeOperatorEnabled

    parser.removeErrorListeners()
    parser.setErrorHandler(new BailErrorStrategy)
  }

  /** Returns the index of the next non-hidden, non-EOF token at or after `from`, or -1. */
  private def nextSignificantTokenIndex(stream: CommonTokenStream, from: Int): Int = {
    var i = from
    while (i < stream.size()) {
      val tok = stream.get(i)
      if (tok.getType == Token.EOF) return -1
      if (tok.getChannel != Token.HIDDEN_CHANNEL) return i
      i += 1
    }
    -1
  }
}
