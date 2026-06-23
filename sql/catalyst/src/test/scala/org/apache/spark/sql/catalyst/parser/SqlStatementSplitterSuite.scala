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

import org.apache.spark.SparkFunSuite

/**
 * Tests for [[SqlStatementSplitter]]. Modeled after Trino's
 * `io.trino.cli.lexer.TestStatementSplitter`, with Spark-specific additions for
 * SQL scripting compound blocks (`BEGIN ... END`).
 */
class SqlStatementSplitterSuite extends SparkFunSuite {

  private def split(sql: String): SqlStatementSplitResult = SqlStatementSplitter.split(sql)

  /** Helper to build a [[SqlStatement]] with the default `;` terminator. */
  private def statement(text: String): SqlStatement = SqlStatement(text, ";")

  // ----------------------------------------------------------------------------------
  // Basic splitting
  // ----------------------------------------------------------------------------------

  test("incomplete - trailing input without delimiter") {
    val result = split(" select * FROM foo  ")
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == "select * FROM foo")
  }

  test("empty input") {
    val result = split("")
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement.isEmpty)
    assert(result.isEmpty)
  }

  test("input with only semicolons") {
    val result = split(";;;")
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement.isEmpty)
  }

  test("single complete statement") {
    val result = split("select * from foo;")
    assert(result.completeStatements == Seq(statement("select * from foo")))
    assert(result.partialStatement.isEmpty)
  }

  test("multiple statements with trailing partial") {
    val result = split(" select * from  foo ; select * from t; select * from ")
    assert(result.completeStatements == Seq(
      statement("select * from  foo"),
      statement("select * from t")))
    assert(result.partialStatement == "select * from")
  }

  test("multiple statements with empty separators") {
    val result = split("; select * from  foo ; select * from t;;;select * from ")
    assert(result.completeStatements == Seq(
      statement("select * from  foo"),
      statement("select * from t")))
    assert(result.partialStatement == "select * from")
  }

  // ----------------------------------------------------------------------------------
  // Error tolerance (mirrors Trino behavior)
  // ----------------------------------------------------------------------------------

  test("unrecognized character before complete statement") {
    val result = split(" select * from z# oops ; select ")
    assert(result.completeStatements == Seq(statement("select * from z# oops")))
    assert(result.partialStatement == "select")
  }

  test("unrecognized character in partial after complete") {
    val result = split("select * from foo; select z# oops ")
    assert(result.completeStatements == Seq(statement("select * from foo")))
    assert(result.partialStatement == "select z# oops")
  }

  test("invalid select with comma before delimiter") {
    val result = split("select abc, ; select 456;")
    assert(result.completeStatements == Seq(
      statement("select abc,"),
      statement("select 456")))
    assert(result.partialStatement.isEmpty)
  }

  // ----------------------------------------------------------------------------------
  // Quoted strings
  // ----------------------------------------------------------------------------------

  test("quoted string with embedded semicolon") {
    val sql = "select ';foo;bar;' x from dual"
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == sql)
  }

  test("complete statement with quoted string containing embedded semicolon") {
    val sql = "select 'foo;bar' x from dual;"
    val result = split(sql)
    assert(result.completeStatements == Seq(statement("select 'foo;bar' x from dual")))
    assert(result.partialStatement.isEmpty)
  }

  test("unterminated quoted string is partial") {
    val sql = "select 'foo', 'bar"
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == sql)
  }

  test("escaped single quote inside string literal") {
    val sql = "select 'hello''world' from dual"
    val result = split(sql + ";")
    assert(result.completeStatements == Seq(statement(sql)))
    assert(result.partialStatement.isEmpty)
  }

  test("quoted identifier with backticks containing semicolon") {
    val sql = "select `f;o;o`, `b``ar` from t"
    val result = split(sql + ";")
    assert(result.completeStatements == Seq(statement(sql)))
    assert(result.partialStatement.isEmpty)
  }

  test("multiple statements with quoted-string semicolons") {
    val result = split("SELECT 'a;b'; SELECT \"c;d\";")
    assert(result.completeStatements == Seq(
      statement("SELECT 'a;b'"),
      statement("SELECT \"c;d\"")))
    assert(result.partialStatement.isEmpty)
  }

  // ----------------------------------------------------------------------------------
  // Comments
  // ----------------------------------------------------------------------------------

  test("single-line comments - comment-only chunks are dropped") {
    // Unlike Trino, the Spark splitter drops comment-only chunks (consistent with
    // the long-standing spark-sql CLI behavior); only the second chunk that has
    // actual SQL content is emitted as a complete statement.
    val result =
      split("--empty\n;-- start\nselect * -- junk\n-- hi\nfrom foo; -- done")
    assert(result.completeStatements == Seq(
      statement("-- start\nselect * -- junk\n-- hi\nfrom foo")))
    assert(result.partialStatement.isEmpty)
  }

  test("multi-line comments - comment-only chunks are dropped") {
    val result =
      split("/* empty */;/* start */ select * /* middle */ from foo; /* end */")
    assert(result.completeStatements == Seq(
      statement("/* start */ select * /* middle */ from foo")))
    assert(result.partialStatement.isEmpty)
  }

  test("single-line comment as partial is dropped (no SQL content)") {
    val sql = "-- start\nselect * -- junk\n-- hi\nfrom foo -- done"
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    // The whole input has SQL content ("select * from foo"), so it is preserved
    // as the partial statement.
    assert(result.partialStatement == sql)
  }

  test("multi-line comment as partial preserves SQL content") {
    val sql = "/* start */ select * /* middle */ from foo /* end */"
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == sql)
  }

  test("nested bracketed comments - comment-only chunk between delimiters dropped") {
    val result = split("SELECT 1; /* outer /* inner */ */;")
    assert(result.completeStatements == Seq(statement("SELECT 1")))
    assert(result.partialStatement.isEmpty)
  }

  test("statement ending with bracketed comment retained") {
    val result = split("SELECT 1; SELECT 2 /* trailer */")
    assert(result.completeStatements == Seq(statement("SELECT 1")))
    assert(result.partialStatement == "SELECT 2 /* trailer */")
  }

  test("unterminated bracketed comment as partial") {
    val result = split("SELECT 1; /* unterminated")
    assert(result.completeStatements == Seq(statement("SELECT 1")))
    assert(result.partialStatement == "/* unterminated")
  }

  // ----------------------------------------------------------------------------------
  // BEGIN..END compound blocks (Spark SQL scripting)
  // ----------------------------------------------------------------------------------

  test("BEGIN..END block is a single statement") {
    val block = "BEGIN SELECT 1; SELECT 2; END"
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block followed by another statement") {
    val block = "BEGIN SELECT 1; END"
    val result = split(block + "; SELECT 99;")
    assert(result.completeStatements == Seq(
      statement(block),
      statement("SELECT 99")))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block preceded by another statement") {
    val block = "BEGIN SELECT 1; SELECT 2; END"
    val result = split("SELECT 11; " + block + ";")
    assert(result.completeStatements == Seq(
      statement("SELECT 11"),
      statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN NOT ATOMIC block") {
    val block = "BEGIN NOT ATOMIC SELECT 1; SELECT 2; END"
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("nested BEGIN..END block") {
    val inner = "BEGIN SELECT 11; SELECT 22; END"
    val block = s"BEGIN $inner; SELECT 33; END"
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block with control flow (IF / WHILE / DECLARE)") {
    val block =
      """BEGIN
        |  DECLARE c INT;
        |  SET c = 0;
        |  WHILE c < 5 DO
        |    SET c = c + 1;
        |  END WHILE;
        |  IF c = 5 THEN
        |    SELECT c;
        |  END IF;
        |END""".stripMargin
    val result = split(block + ";\nSELECT 100;")
    assert(result.completeStatements == Seq(
      statement(block),
      statement("SELECT 100")))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block with EXIT HANDLER FOR SQLEXCEPTION") {
    // The exception handler body is itself a `BEGIN ... END` containing a `;`.
    // The outer `END` is the boundary of the whole compound block.
    val block =
      """BEGIN
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |  BEGIN
        |    VALUES('Hello from the exception handler.');
        |  END;
        |
        |  SELECT * FROM does_not_exist;
        |END""".stripMargin
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block with CONTINUE HANDLER FOR SQLSTATE") {
    val block =
      """BEGIN
        |  DECLARE CONTINUE HANDLER FOR SQLSTATE '22012'
        |    SELECT 'caught divide by zero';
        |  SELECT 1 / 0;
        |  SELECT 'after handler';
        |END""".stripMargin
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block with single-statement handler body (SET)") {
    // The handler body can be a single statement instead of a `BEGIN ... END`
    // per the grammar:
    //    DECLARE ... HANDLER FOR ...
    //    (beginEndCompoundBlock | statement | setStatementInsideSqlScript)
    val block =
      """BEGIN
        |  DECLARE flag INT DEFAULT 0;
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION SET flag = 1;
        |  SELECT * FROM does_not_exist;
        |END""".stripMargin
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block with handlers for multiple condition values") {
    val block =
      """BEGIN
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION, SQLSTATE '22012', NOT FOUND
        |  BEGIN
        |    SELECT 'caught';
        |  END;
        |  SELECT 1 / 0;
        |END""".stripMargin
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN..END block with nested compound + handler followed by another statement") {
    val block =
      """BEGIN
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |  BEGIN
        |    VALUES('Hello from the exception handler.');
        |  END;
        |
        |  SELECT * FROM does_not_exist;
        |END""".stripMargin
    val result = split(s"$block;\nSELECT 'after';")
    assert(result.completeStatements == Seq(
      statement(block),
      statement("SELECT 'after'")))
    assert(result.partialStatement.isEmpty)
  }

  test("incomplete BEGIN..END block with EXIT HANDLER - treated as partial") {
    // Same body as the canonical handler example, but missing the outer `END`.
    // The parser cannot confirm the block, and -- critically -- the embedded
    // `END;` of the handler body does NOT cause us to emit a premature
    // statement; the entire input is buffered as a partial.
    val sql =
      """BEGIN
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |  BEGIN
        |    VALUES('Hello from the exception handler.');
        |  END;
        |
        |  SELECT * FROM does_not_exist;""".stripMargin
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == sql)
  }

  test("BEGIN..END block ending without trailing delimiter is partial") {
    val block = "BEGIN SELECT 1; END"
    val result = split(block)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == block)
  }

  test("incomplete BEGIN..END block (no END) - treated as partial") {
    val sql = "BEGIN SELECT 1; SELECT 2;"
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == "BEGIN SELECT 1; SELECT 2;")
  }

  test("incomplete BEGIN..END block across multiple inputs - absorbs everything") {
    // Simulates an interactive CLI accumulating input. As long as END is missing
    // the splitter holds everything as partial so the caller can keep buffering.
    val r1 = split("BEGIN\n")
    assert(r1.completeStatements.isEmpty)
    assert(r1.partialStatement == "BEGIN")
    val r2 = split("BEGIN\nSELECT 1;\nSELECT 2;\n")
    assert(r2.completeStatements.isEmpty)
    assert(r2.partialStatement.startsWith("BEGIN"))
    assert(r2.partialStatement.endsWith(";"))
    val r3 = split("BEGIN\nSELECT 1;\nSELECT 2;\nEND;\n")
    assert(r3.completeStatements ==
      Seq(statement("BEGIN\nSELECT 1;\nSELECT 2;\nEND")))
    assert(r3.partialStatement.isEmpty)
  }

  test("BEGIN..END block with embedded line comments and block comments") {
    val block =
      """BEGIN -- start
        |  /* declare */
        |  SELECT 1; -- pick a number
        |  SELECT 2; /* another */
        |END /* tail */""".stripMargin
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN identifier (lower-cased) is matched") {
    // BEGIN keyword is non-reserved; it can be written in any case.
    val block = "begin select 1; select 2; end"
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN used as identifier in middle of statement does not trigger block parse") {
    // BEGIN is non-reserved, so it may appear as a column reference. As long as
    // it is NOT the first significant token in a statement, block detection
    // should not engage.
    val sql = "SELECT begin FROM t WHERE x = 1;"
    val result = split(sql)
    assert(result.completeStatements == Seq(statement("SELECT begin FROM t WHERE x = 1")))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN as identifier in first statement, followed by compound block") {
    // The first statement uses `begin` as a column identifier (valid since BEGIN
    // is a non-reserved keyword). The second statement is a real compound block.
    // The splitter must correctly disambiguate: BEGIN at the *start* of a
    // statement triggers compound-block detection, otherwise it's just a token.
    val block = "BEGIN SELECT 1; SELECT 2; END"
    val sql = s"select begin from t; $block;"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("select begin from t"),
      statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("multiple BEGIN..END blocks separated by other statements using BEGIN as identifier") {
    val block1 = "BEGIN SELECT 1; END"
    val block2 = "BEGIN SELECT 2; END"
    val sql =
      s"select begin from t1; $block1; select begin as begin from t2; $block2;"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("select begin from t1"),
      statement(block1),
      statement("select begin as begin from t2"),
      statement(block2)))
    assert(result.partialStatement.isEmpty)
  }

  test("BEGIN-led invalid body cannot be confirmed by parser and becomes partial") {
    // With the parser-based splitter, a BEGIN-led input that the parser cannot
    // confirm as a valid compound block (no matching END found) is buffered
    // as a partial statement. Interactive callers either type END to complete
    // it, or trigger an EOF flush which forwards the partial to the backend
    // for a proper parse error. (This differs from Trino's `containsFunction`
    // shortcut, which can fall back to per-`;` splitting for invalid bodies
    // because Trino's grammar lacks the `.*?` wildcards that Spark's
    // `setResetStatement` relies on.)
    val sql = "BEGIN FROM t; SELECT 1;"
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == sql)
  }

  test("BEGIN immediately followed by ; - failedNonEof path falls through safely") {
    // `BEGIN;` is structurally invalid: the parser bails as soon as it sees
    // `;` right after `BEGIN` (LA(1) after fail = `;`, i.e. FailedNonEof).
    // The fallback delimiter walk emits `BEGIN` as one (broken) statement and
    // continues with the next.
    //
    // This exercises the failedNonEof branch with a BEGIN-led input -- it is
    // important that this case does NOT incorrectly split a *valid* compound
    // block. Valid blocks always return ParsedOk, so the fall-through here
    // can only happen for inputs that aren't valid compound blocks anyway.
    val result = split("BEGIN; SELECT 1;")
    assert(result.completeStatements == Seq(
      statement("BEGIN"),
      statement("SELECT 1")))
    assert(result.partialStatement.isEmpty)
  }

  test("Valid BEGIN..END block is never split at internal ;") {
    // A valid `BEGIN ... END` block must be confirmed in full -- the splitter
    // must never emit at an internal `;`, even if a shorter prefix happens to
    // look like a valid statement to a less-careful splitter.
    val block =
      """BEGIN
        |  SELECT 1;
        |  SELECT 2;
        |  SELECT 3;
        |END""".stripMargin
    val result = split(block + ";")
    assert(result.completeStatements == Seq(statement(block)))
    assert(result.partialStatement.isEmpty)
  }

  test("Deeply nested expression does not propagate StackOverflowError") {
    // Deeply nested parenthesized expressions can cause the ANTLR parser to
    // throw [[StackOverflowError]] during recursive descent. The splitter
    // must catch it (and not propagate it out of `split`) so the caller
    // still gets a result rather than a crash.
    val deepExpr = "(" * 5000 + "1" + ")" * 5000
    val sql = s"SELECT $deepExpr; SELECT 2;"
    // Even if the deeply-nested first statement fails to parse, the splitter
    // must still return without throwing -- the backend can later report the
    // actual error via `QueryParsingErrors.parserStackOverflow`.
    val result = split(sql)
    // We don't assert on the specific contents -- depending on parser limits
    // the first chunk may be emitted as a broken statement or buffered as a
    // partial. What we DO require is that the call returns normally.
    assert(result != null)
  }

  test("empty compound block BEGIN END is a single statement") {
    val result = split("BEGIN END; SELECT 1;")
    assert(result.completeStatements == Seq(
      statement("BEGIN END"),
      statement("SELECT 1")))
    assert(result.partialStatement.isEmpty)
  }

  // ----------------------------------------------------------------------------------
  // Multiple statements in mixed form
  // ----------------------------------------------------------------------------------

  test("multiple statements - mix of regular and BEGIN..END") {
    val block = "BEGIN SELECT 1; SELECT 2; END"
    val sql = s"SELECT 11; $block; SELECT 22; $block; SELECT 33;"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("SELECT 11"),
      statement(block),
      statement("SELECT 22"),
      statement(block),
      statement("SELECT 33")))
    assert(result.partialStatement.isEmpty)
  }

  // ----------------------------------------------------------------------------------
  // CLI-flavored edge cases for spark-sql
  // ----------------------------------------------------------------------------------

  test("CLI: dollar-quoted string is preserved") {
    val sql = "SELECT $tag$ a;b;c $tag$ FROM t;"
    val result = split(sql)
    assert(result.completeStatements == Seq(statement("SELECT $tag$ a;b;c $tag$ FROM t")))
    assert(result.partialStatement.isEmpty)
  }

  test("CLI: SET command with trailing semicolons") {
    val result = split("SET spark.foo=1; SET spark.bar=2;")
    assert(result.completeStatements == Seq(
      statement("SET spark.foo=1"),
      statement("SET spark.bar=2")))
    assert(result.partialStatement.isEmpty)
  }

  test("CLI: SPARK-37906 - comment-only trailing input is dropped") {
    // The CLI relies on these results not producing spurious empty statements.
    assert(split("SELECT 1; --comment").completeStatements ==
      Seq(statement("SELECT 1")))
    assert(split("-- comment ").completeStatements.isEmpty)
    assert(split("/*  comment */  ").completeStatements.isEmpty)
    assert(split("-- comment \nSELECT 1").partialStatement == "-- comment \nSELECT 1")
  }

  test("CLI: SPARK-54876 - assorted comment / split edge cases") {
    assert(split("SELECT 1; SELECT 2 /* comment */") ==
      SqlStatementSplitResult(Seq(statement("SELECT 1")), "SELECT 2 /* comment */"))
    assert(split("-- foo\n/* bar */") ==
      SqlStatementSplitResult(Nil, ""))
    assert(split("SELECT 1; -- foo\n /* bar */") ==
      SqlStatementSplitResult(Seq(statement("SELECT 1")), ""))
    assert(split("SELECT 1; /* outer /* inner */ */") ==
      SqlStatementSplitResult(Seq(statement("SELECT 1")), ""))
    assert(split("/* a */ -- foo\n/* b */") ==
      SqlStatementSplitResult(Nil, ""))
    // Unterminated comments set hasUnclosedComment = true.
    assert(split("SELECT 1; /* unterminated") ==
      SqlStatementSplitResult(
        Seq(statement("SELECT 1")), "/* unterminated", hasUnclosedComment = true))
    assert(split("'unterminated") ==
      SqlStatementSplitResult(Nil, "'unterminated"))
    assert(split("SELECT 1; 'unterminated string") ==
      SqlStatementSplitResult(Seq(statement("SELECT 1")), "'unterminated string"))
    assert(split("/* only a comment that never closes") ==
      SqlStatementSplitResult(
        Nil, "/* only a comment that never closes", hasUnclosedComment = true))
    assert(split("SELECT * FROM `t;a`; SELECT 1") ==
      SqlStatementSplitResult(Seq(statement("SELECT * FROM `t;a`")), "SELECT 1"))
  }

  // ----------------------------------------------------------------------------------
  // Validation
  // ----------------------------------------------------------------------------------

  test("rejects null SQL text") {
    intercept[IllegalArgumentException](split(null))
  }

  // ----------------------------------------------------------------------------------
  // Integration with ParserInterface
  // ----------------------------------------------------------------------------------

  test("ParserInterface.splitStatements delegates to SqlStatementSplitter") {
    val parser = new CatalystSqlParser
    val result = parser.splitStatements("SELECT 1; SELECT 2;")
    assert(result.completeStatements == Seq(statement("SELECT 1"), statement("SELECT 2")))
    assert(result.partialStatement.isEmpty)
  }

  test("SqlStatement.toString concatenates statement and terminator") {
    assert(SqlStatement("SELECT 1", ";").toString == "SELECT 1;")
  }

  test("SqlStatementSplitResult.isEmpty") {
    assert(SqlStatementSplitResult(Nil, "").isEmpty)
    assert(!SqlStatementSplitResult(Seq(SqlStatement("x", ";")), "").isEmpty)
    assert(!SqlStatementSplitResult(Nil, "x").isEmpty)
  }

  test("hasUnclosedComment flag - set only for unclosed bracketed comments in partial") {
    assert(split("/* unclosed").hasUnclosedComment)
    assert(split("SELECT 1; /* unclosed").hasUnclosedComment)
    assert(split("SELECT 1; /* unclosed comment SELECT 2;").hasUnclosedComment)
    // Closed comments do not set the flag.
    assert(!split("/* closed */").hasUnclosedComment)
    assert(!split("SELECT 1; /* closed */").hasUnclosedComment)
    assert(!split("/* nested /* still nested */ outer */").hasUnclosedComment)
    // No comments at all.
    assert(!split("SELECT 1").hasUnclosedComment)
    assert(!split("SELECT 1;").hasUnclosedComment)
  }

  test("SPARK-31595 - unescaped quote mark in quoted string") {
    // The `"` inside a single-quoted string is a literal `"`, not an escape.
    val result = split("SELECT '\"legal string a';select 1 + 234;")
    assert(result.completeStatements == Seq(
      statement("SELECT '\"legal string a'"),
      statement("select 1 + 234")))
    assert(result.partialStatement.isEmpty)
  }

  // ----------------------------------------------------------------------------------
  // Extension grammars (e.g. Iceberg's `IcebergSparkSqlExtensionsParser`)
  // ----------------------------------------------------------------------------------
  //
  // The splitter validates each candidate statement against Spark's *vanilla*
  // SQL grammar, not against any session-injected parser extension. Inputs that
  // the vanilla parser rejects still need to be split correctly so that the
  // CLI can hand each chunk to the extended parser for execution.
  //
  // The splitter achieves this via its `FailedNonEof` fall-back: when the
  // vanilla parser cannot consume a candidate prefix, the splitter reverts to
  // a token-level walk and emits at the next `;` -- matching the behavior of
  // the old `StringUtils.splitSemiColon` scanner exactly. Quoted strings and
  // comments are still honored (they are part of the lexer, not the parser),
  // so semicolons inside them remain non-delimiters.

  test("Iceberg-style ALTER TABLE single statement splits correctly") {
    // `ADD PARTITION FIELD bucket(...)` is not in vanilla Spark SQL but is
    // provided by Iceberg's grammar extension. The vanilla parser rejects it
    // (the splitter's per-candidate `compoundOrSingleStatement` call fails),
    // so the splitter falls back to delimiter splitting and the extended
    // parser then handles the actual execution.
    val sql = "ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id);"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE prod.db.sample ADD PARTITION FIELD bucket(16, id)")))
    assert(result.partialStatement.isEmpty)
  }

  test("Iceberg-style statement followed by vanilla statement") {
    val sql =
      "ALTER TABLE t ADD PARTITION FIELD bucket(16, id); SELECT 1;"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE t ADD PARTITION FIELD bucket(16, id)"),
      statement("SELECT 1")))
    assert(result.partialStatement.isEmpty)
  }

  test("Iceberg-style WRITE ORDERED BY clause splits correctly") {
    val sql = "ALTER TABLE t WRITE ORDERED BY id;"
    val result = split(sql)
    assert(result.completeStatements == Seq(statement("ALTER TABLE t WRITE ORDERED BY id")))
    assert(result.partialStatement.isEmpty)
  }

  test("Multiple Iceberg-style statements split at every ;") {
    val sql =
      """ALTER TABLE t1 ADD PARTITION FIELD bucket(16, id);
        |ALTER TABLE t2 ADD PARTITION FIELD truncate(10, name);
        |ALTER TABLE t3 WRITE ORDERED BY id;""".stripMargin
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE t1 ADD PARTITION FIELD bucket(16, id)"),
      statement("ALTER TABLE t2 ADD PARTITION FIELD truncate(10, name)"),
      statement("ALTER TABLE t3 WRITE ORDERED BY id")))
    assert(result.partialStatement.isEmpty)
  }

  test("Extension statement with ; inside a quoted string is not split") {
    // The lexer correctly tokenizes the embedded `;` as part of the
    // STRING_LITERAL, so even when the splitter falls back to delimiter
    // scanning, semicolons inside quotes remain non-delimiters.
    val sql = "ALTER TABLE t SET TBLPROPERTIES ('foo' = 'a; b; c');"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE t SET TBLPROPERTIES ('foo' = 'a; b; c')")))
    assert(result.partialStatement.isEmpty)
  }

  test("Extension fall-back: ; inside a STRING_LITERAL is not a delimiter") {
    // The vanilla parser fails on `ADD PARTITION FIELD names(...)` so the
    // splitter takes the failedNonEof fall-back. The fall-back walks tokens
    // and checks `token.getType == SEMICOLON`; the `;`s inside `'a;b;c'` are
    // part of a single STRING_LITERAL token (not separate SEMICOLON tokens),
    // so they are correctly preserved -- mirroring the behavior of the old
    // `StringUtils.splitSemiColon` scanner.
    val sql = "ALTER TABLE t ADD PARTITION FIELD names('a;b;c');"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE t ADD PARTITION FIELD names('a;b;c')")))
    assert(result.partialStatement.isEmpty)
  }

  test("Extension fall-back: ; inside a `backtick-quoted identifier` is not a delimiter") {
    val sql = "ALTER TABLE `t;weird` ADD PARTITION FIELD bucket(16, id);"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE `t;weird` ADD PARTITION FIELD bucket(16, id)")))
    assert(result.partialStatement.isEmpty)
  }

  test("Extension fall-back: ; inside a single-line comment is not a delimiter") {
    val sql =
      """ALTER TABLE t ADD PARTITION FIELD bucket(16, id); -- trailing ; comment
        |ALTER TABLE t WRITE ORDERED BY id;""".stripMargin
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE t ADD PARTITION FIELD bucket(16, id)"),
      statement("-- trailing ; comment\nALTER TABLE t WRITE ORDERED BY id")))
    assert(result.partialStatement.isEmpty)
  }

  test("Extension fall-back: ; inside a bracketed comment is not a delimiter") {
    val sql = "ALTER TABLE t ADD PARTITION FIELD bucket(/* in ; comment */ 16, id);"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE t ADD PARTITION FIELD bucket(/* in ; comment */ 16, id)")))
    assert(result.partialStatement.isEmpty)
  }

  test("Extension fall-back: ; inside a nested bracketed comment is not a delimiter") {
    val sql = "ALTER TABLE t ADD PARTITION FIELD bucket(/* outer /* inner ; */ ; */ 16, id);"
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement(
        "ALTER TABLE t ADD PARTITION FIELD bucket(/* outer /* inner ; */ ; */ 16, id)")))
    assert(result.partialStatement.isEmpty)
  }

  test("Extension statement without trailing ; is partial") {
    // No `;` means there are no delimiter candidates to try, so the splitter
    // falls into the "no more delimiters" branch and surfaces the input as a
    // partial. The CLI buffers and waits for more input.
    val sql = "ALTER TABLE t ADD PARTITION FIELD bucket(16, id)"
    val result = split(sql)
    assert(result.completeStatements.isEmpty)
    assert(result.partialStatement == sql)
  }

  // ----------------------------------------------------------------------------------
  // Mixed vanilla SQL, vanilla SQL scripting, and extension SQL
  // ----------------------------------------------------------------------------------
  //
  // Inputs that mix all three flavors must split correctly:
  //   - Vanilla statements (SELECT, INSERT, ...) -> ParsedOk via vanilla parser.
  //   - Vanilla compound `BEGIN ... END` blocks -> ParsedOk after extending the
  //     prefix to include the closing `END;` (embedded `;` are not split-points).
  //   - Extension statements (e.g. Iceberg) -> FailedNonEof at the first
  //     unexpected token, then the fall-back walk splits at the next `;`
  //     while honoring strings/comments.

  test("Mix: vanilla + extension + vanilla") {
    val sql =
      """SELECT 1;
        |ALTER TABLE prod.db.t ADD PARTITION FIELD bucket(16, id);
        |SELECT 2;""".stripMargin
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("SELECT 1"),
      statement("ALTER TABLE prod.db.t ADD PARTITION FIELD bucket(16, id)"),
      statement("SELECT 2")))
    assert(result.partialStatement.isEmpty)
  }

  test("Mix: vanilla BEGIN..END block + extension + vanilla statement") {
    // The compound block must remain one statement (its internal `;` are
    // *not* split-points) even when followed by an extension statement that
    // forces the fall-back walk for the rest of the input.
    val block = "BEGIN SELECT 1; SELECT 2; END"
    val sql =
      s"""$block;
         |ALTER TABLE t ADD PARTITION FIELD bucket(16, id);
         |SELECT 99;""".stripMargin
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement(block),
      statement("ALTER TABLE t ADD PARTITION FIELD bucket(16, id)"),
      statement("SELECT 99")))
    assert(result.partialStatement.isEmpty)
  }

  test("Mix: extension + vanilla BEGIN..END + extension") {
    // Order reversed: extension first, then a real compound block, then
    // another extension. The compound block must still be confirmed as one
    // statement by the parser, not split at its internal `;`.
    val block = "BEGIN SELECT 1; SELECT 2; END"
    val sql =
      s"""ALTER TABLE t1 ADD PARTITION FIELD bucket(16, id);
         |$block;
         |ALTER TABLE t2 WRITE ORDERED BY id;""".stripMargin
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement("ALTER TABLE t1 ADD PARTITION FIELD bucket(16, id)"),
      statement(block),
      statement("ALTER TABLE t2 WRITE ORDERED BY id")))
    assert(result.partialStatement.isEmpty)
  }

  test("Mix: vanilla SQL scripting compound block with EXIT HANDLER + extension after") {
    // SQL scripting block (vanilla) with an exception handler containing its
    // own embedded `;`s, followed by an extension statement. The compound
    // block is ParsedOk in full; the trailing extension statement falls back
    // to the delimiter walk.
    val block =
      """BEGIN
        |  DECLARE EXIT HANDLER FOR SQLEXCEPTION
        |  BEGIN
        |    VALUES('Hello from the exception handler.');
        |  END;
        |
        |  SELECT * FROM does_not_exist;
        |END""".stripMargin
    val sql =
      s"""$block;
         |ALTER TABLE t ADD PARTITION FIELD bucket(16, id);""".stripMargin
    val result = split(sql)
    assert(result.completeStatements == Seq(
      statement(block),
      statement("ALTER TABLE t ADD PARTITION FIELD bucket(16, id)")))
    assert(result.partialStatement.isEmpty)
  }

  test("Mix: extensions, BEGIN..END, comments, and strings interleaved") {
    // A torture-test mix: line comments, block comments, single- and
    // double-quoted strings, back-ticked identifiers, vanilla DML, an
    // extension DDL with embedded `;` inside a comment, and a vanilla
    // BEGIN..END compound block, all with various trailing `;`s.
    val block = "BEGIN SELECT 'inside ; block'; SELECT 2; END"
    val sql =
      s"""-- prelude
         |/* multi
         |   line
         |   comment with ; in it */
         |SELECT 'a;b;c', "x;y;z", `col;name` FROM `t;weird`;
         |
         |ALTER TABLE t ADD PARTITION FIELD bucket(/* ; in comment */ 16, id);
         |
         |$block;
         |
         |-- trailing comment with ; in it
         |""".stripMargin
    val result = split(sql)
    assert(result.completeStatements.map(_.statement) === Seq(
      // The leading line comment + block comment carry over into the first
      // emitted statement (the splitter preserves leading hidden tokens when
      // a `parsedOk` candidate is found).
      """-- prelude
        |/* multi
        |   line
        |   comment with ; in it */
        |SELECT 'a;b;c', "x;y;z", `col;name` FROM `t;weird`""".stripMargin,
      "ALTER TABLE t ADD PARTITION FIELD bucket(/* ; in comment */ 16, id)",
      block))
    assert(result.partialStatement.isEmpty)
  }

  test("Mix: multiple BEGIN..END blocks with extensions and vanilla in between") {
    val block1 = "BEGIN SELECT 1; SELECT 2; END"
    val block2 = "BEGIN SELECT 3; SELECT 4; END"
    val sql =
      s"""SELECT 'first';
         |$block1;
         |ALTER TABLE t1 ADD PARTITION FIELD bucket(16, id);
         |INSERT INTO t2 VALUES (1);
         |$block2;
         |ALTER TABLE t3 WRITE ORDERED BY id;
         |SELECT 'last';""".stripMargin
    val result = split(sql)
    assert(result.completeStatements.map(_.statement) === Seq(
      "SELECT 'first'",
      block1,
      "ALTER TABLE t1 ADD PARTITION FIELD bucket(16, id)",
      "INSERT INTO t2 VALUES (1)",
      block2,
      "ALTER TABLE t3 WRITE ORDERED BY id",
      "SELECT 'last'"))
    assert(result.partialStatement.isEmpty)
  }

  test("Mix: incomplete extension at end keeps the rest as partial-friendly buffer") {
    // The vanilla statements before the trailing extension are emitted; the
    // un-terminated extension at the end becomes the partial. (No `;` after
    // the last `bucket(...)` -> "no more delimiters" branch.)
    val sql =
      """SELECT 1;
        |BEGIN SELECT 2; END;
        |ALTER TABLE t ADD PARTITION FIELD bucket(16, id)""".stripMargin
    val result = split(sql)
    assert(result.completeStatements.map(_.statement) === Seq(
      "SELECT 1",
      "BEGIN SELECT 2; END"))
    assert(result.partialStatement ==
      "ALTER TABLE t ADD PARTITION FIELD bucket(16, id)")
  }

  test("Mix: extension inside a BEGIN..END body splits at internal ; (documented limitation)") {
    // This input is meant to be a valid SQL-scripting block whose body uses
    // an extension statement. The splitter's vanilla parser cannot recognise
    // the extension, so it cannot confirm the whole block as a single
    // compound statement and falls back to per-`;` splitting. The result is
    // multiple broken chunks rather than one block -- a known limitation
    // (the same as the old `StringUtils.splitSemiColon` behavior, which also
    // could not preserve compound-block boundaries).
    val sql =
      """BEGIN
        |  ALTER TABLE t ADD PARTITION FIELD bucket(16, id);
        |  SELECT 1;
        |END;""".stripMargin
    val result = split(sql)
    // We don't assert the exact split here because the precise chunk
    // boundaries depend on where the vanilla parser bails. What we DO
    // require is that the call returns -- the splitter must remain
    // fault-tolerant for invalid (or extension-laden) bodies.
    assert(result != null)
    // The input is at least split at SOME `;` boundary; we don't get one
    // single statement back.
    assert(result.completeStatements.size + (if (result.partialStatement.isEmpty) 0 else 1) >= 2)
  }
}
