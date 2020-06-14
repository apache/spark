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

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.fileToString
import org.apache.spark.sql.internal.SQLConf

class TableIdentifierParserSuite extends SparkFunSuite with SQLHelper {
  import CatalystSqlParser._

  // Add "$elem$", "$value$" & "$key$"
  // It is recommended to list them in alphabetical order.
  val hiveNonReservedKeyword = Array(
    "add",
    "admin",
    "after",
    "all",
    "alter",
    "analyze",
    "any",
    "archive",
    "array",
    "as",
    "asc",
    "at",
    "authorization",
    "autocommit",
    "before",
    "between",
    "bigint",
    "binary",
    "boolean",
    "both",
    "bucket",
    "buckets",
    "by",
    "cascade",
    "change",
    "cluster",
    "clustered",
    "clusterstatus",
    "collection",
    "columns",
    "comment",
    "compact",
    "compactions",
    "compute",
    "concatenate",
    "continue",
    "cost",
    "create",
    "cube",
    "current_date",
    "current_timestamp",
    "cursor",
    "data",
    "databases",
    "date",
    "datetime",
    "day",
    "dbproperties",
    "decimal",
    "deferred",
    "defined",
    "delete",
    "delimited",
    "dependency",
    "desc",
    "describe",
    "directories",
    "directory",
    "disable",
    "distribute",
    "double",
    "drop",
    "enable",
    "escaped",
    "exclusive",
    "exists",
    "explain",
    "export",
    "external",
    "extract",
    "false",
    "fetch",
    "fields",
    "file",
    "fileformat",
    "first",
    "float",
    "for",
    "format",
    "formatted",
    "functions",
    "grant",
    "group",
    "grouping",
    "hold_ddltime",
    "hour",
    "idxproperties",
    "ignore",
    "import",
    "in",
    "index",
    "indexes",
    "inpath",
    "inputdriver",
    "inputformat",
    "insert",
    "int",
    "into",
    "is",
    "isolation",
    "items",
    "jar",
    "key_type",
    "keys",
    "last",
    "lateral",
    "leading",
    "level",
    "like",
    "limit",
    "lines",
    "load",
    "local",
    "location",
    "lock",
    "locks",
    "logical",
    "long",
    "mapjoin",
    "materialized",
    "metadata",
    "minus",
    "minute",
    "month",
    "msck",
    "no_drop",
    "none",
    "noscan",
    "null",
    "nulls",
    "of",
    "offline",
    "offset",
    "option",
    "order",
    "out",
    "outer",
    "outputdriver",
    "outputformat",
    "overwrite",
    "owner",
    "partition",
    "partitioned",
    "partitions",
    "percent",
    "pivot",
    "plus",
    "position",
    "pretty",
    "principals",
    "procedure",
    "protection",
    "purge",
    "query",
    "range",
    "read",
    "readonly",
    "reads",
    "rebuild",
    "recordreader",
    "recordwriter",
    "regexp",
    "reload",
    "rename",
    "repair",
    "replace",
    "replication",
    "restrict",
    "revoke",
    "rewrite",
    "rlike",
    "role",
    "roles",
    "rollup",
    "row",
    "rows",
    "schemas",
    "second",
    "serde",
    "serdeproperties",
    "server",
    "set",
    "sets",
    "shared",
    "show",
    "show_database",
    "skewed",
    "smallint",
    "snapshot",
    "sort",
    "sorted",
    "ssl",
    "statistics",
    "stored",
    "streamtable",
    "string",
    "struct",
    "table",
    "tables",
    "tblproperties",
    "temporary",
    "terminated",
    "timestamp",
    "tinyint",
    "to",
    "touch",
    "trailing",
    "transaction",
    "transactions",
    "trigger",
    "trim",
    "true",
    "truncate",
    "unarchive",
    "undo",
    "uniontype",
    "unlock",
    "unset",
    "unsigned",
    "update",
    "uri",
    "use",
    "user",
    "utc",
    "utctimestamp",
    "values",
    "view",
    "views",
    "while",
    "with",
    "work",
    "write",
    "year")

  val hiveStrictNonReservedKeyword = Seq(
    "anti",
    "cross",
    "database",
    "except",
    "from",
    "full",
    "having",
    "inner",
    "intersect",
    "join",
    "left",
    "natural",
    "not",
    "on",
    "right",
    "select",
    "semi",
    "table",
    "to",
    "union",
    "where",
    "with")

  private val sqlSyntaxDefs = {
    val sqlBasePath = {
      val sparkHome = {
        assert(sys.props.contains("spark.test.home") ||
          sys.env.contains("SPARK_HOME"), "spark.test.home or SPARK_HOME is not set.")
        sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
      }
      java.nio.file.Paths.get(sparkHome, "sql", "catalyst", "src", "main", "antlr4", "org",
        "apache", "spark", "sql", "catalyst", "parser", "SqlBase.g4").toFile
    }
    fileToString(sqlBasePath).split("\n")
  }

  private def parseAntlrGrammars[T](startTag: String, endTag: String)
      (f: PartialFunction[String, Seq[T]]): Set[T] = {
    val keywords = new mutable.ArrayBuffer[T]
    val default = (_: String) => Nil
    var startTagFound = false
    var parseFinished = false
    val lineIter = sqlSyntaxDefs.toIterator
    while (!parseFinished && lineIter.hasNext) {
      val line = lineIter.next()
      if (line.trim.startsWith(startTag)) {
        startTagFound = true
      } else if (line.trim.startsWith(endTag)) {
        parseFinished = true
      } else if (startTagFound) {
        f.applyOrElse(line, default).foreach { symbol =>
          keywords += symbol
        }
      }
    }
    assert(keywords.nonEmpty && startTagFound && parseFinished, "cannot extract keywords from " +
      s"the `SqlBase.g4` file, so please check if the start/end tags (`$startTag` and `$endTag`) " +
      "are placed correctly in the file.")
    keywords.toSet
  }

  // If a symbol does not have the same string with its literal (e.g., `SETMINUS: 'MINUS';`),
  // we need to map a symbol to actual literal strings.
  val symbolsToExpandIntoDifferentLiterals = {
    val kwDef = """([A-Z_]+):(.+);""".r
    val keywords = parseAntlrGrammars(
        "//--SPARK-KEYWORD-LIST-START", "//--SPARK-KEYWORD-LIST-END") {
      case kwDef(symbol, literalDef) =>
        val splitDefs = literalDef.split("""\|""")
        val hasMultipleLiterals = splitDefs.length > 1
        // The case where a symbol has multiple literal definitions,
        // e.g., `DATABASES: 'DATABASES' | 'SCHEMAS';`.
        if (hasMultipleLiterals) {
          val literals = splitDefs.map(_.replaceAll("'", "").trim).toSeq
          (symbol, literals) :: Nil
        } else {
          val literal = literalDef.replaceAll("'", "").trim
          // The case where a symbol string and its literal string are different,
          // e.g., `SETMINUS: 'MINUS';`.
          if (symbol != literal) {
            (symbol, literal :: Nil) :: Nil
          } else {
            Nil
          }
        }
    }
    keywords.toMap
  }

  // All the SQL keywords defined in `SqlBase.g4`
  val allCandidateKeywords = {
    val kwDef = """([A-Z_]+):.+;""".r
    val keywords = parseAntlrGrammars(
        "//--SPARK-KEYWORD-LIST-START", "//--SPARK-KEYWORD-LIST-END") {
      // Parses a pattern, e.g., `AFTER: 'AFTER';`
      case kwDef(symbol) =>
        if (symbolsToExpandIntoDifferentLiterals.contains(symbol)) {
          symbolsToExpandIntoDifferentLiterals(symbol)
        } else {
          symbol :: Nil
        }
    }
    keywords
  }

  val nonReservedKeywordsInAnsiMode = {
    val kwDef = """\s*[\|:]\s*([A-Z_]+)\s*""".r
    parseAntlrGrammars("//--ANSI-NON-RESERVED-START", "//--ANSI-NON-RESERVED-END") {
      // Parses a pattern, e.g., `    | AFTER`
      case kwDef(symbol) =>
        if (symbolsToExpandIntoDifferentLiterals.contains(symbol)) {
          symbolsToExpandIntoDifferentLiterals(symbol)
        } else {
          symbol :: Nil
        }
    }
  }

  val reservedKeywordsInAnsiMode = allCandidateKeywords -- nonReservedKeywordsInAnsiMode

  test("check # of reserved keywords") {
    val numReservedKeywords = 78
    assert(reservedKeywordsInAnsiMode.size == numReservedKeywords,
      s"The expected number of reserved keywords is $numReservedKeywords, but " +
        s"${reservedKeywordsInAnsiMode.size} found.")
  }

  test("table identifier") {
    // Regular names.
    assert(TableIdentifier("q") === parseTableIdentifier("q"))
    assert(TableIdentifier("q", Option("d")) === parseTableIdentifier("d.q"))

    // Illegal names.
    Seq("", "d.q.g", "t:", "${some.var.x}", "tab:1").foreach { identifier =>
      intercept[ParseException](parseTableIdentifier(identifier))
    }
  }

  test("quoted identifiers") {
    assert(TableIdentifier("z", Some("x.y")) === parseTableIdentifier("`x.y`.z"))
    assert(TableIdentifier("y.z", Some("x")) === parseTableIdentifier("x.`y.z`"))
    assert(TableIdentifier("z", Some("`x.y`")) === parseTableIdentifier("```x.y```.z"))
    assert(TableIdentifier("`y.z`", Some("x")) === parseTableIdentifier("x.```y.z```"))
    assert(TableIdentifier("x.y.z", None) === parseTableIdentifier("`x.y.z`"))
  }

  test("table identifier - reserved/non-reserved keywords if ANSI mode enabled") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      reservedKeywordsInAnsiMode.foreach { keyword =>
        val errMsg = intercept[ParseException] {
          parseTableIdentifier(keyword)
        }.getMessage
        assert(errMsg.contains("no viable alternative at input"))
        assert(TableIdentifier(keyword) === parseTableIdentifier(s"`$keyword`"))
        assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.`$keyword`"))
      }
      nonReservedKeywordsInAnsiMode.foreach { keyword =>
        assert(TableIdentifier(keyword) === parseTableIdentifier(s"$keyword"))
        assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.$keyword"))
      }
    }
  }

  test("table identifier - strict keywords") {
    // SQL Keywords.
    hiveStrictNonReservedKeyword.foreach { keyword =>
      assert(TableIdentifier(keyword) === parseTableIdentifier(keyword))
      assert(TableIdentifier(keyword) === parseTableIdentifier(s"`$keyword`"))
      assert(TableIdentifier(keyword, Option("db")) === parseTableIdentifier(s"db.`$keyword`"))
    }
  }

  test("table identifier - non reserved keywords") {
    // Hive keywords are allowed.
    hiveNonReservedKeyword.foreach { nonReserved =>
      assert(TableIdentifier(nonReserved) === parseTableIdentifier(nonReserved))
    }
  }

  test("SPARK-17364 table identifier - contains number") {
    assert(parseTableIdentifier("123_") == TableIdentifier("123_"))
    assert(parseTableIdentifier("1a.123_") == TableIdentifier("123_", Some("1a")))
    // ".123" should not be treated as token of type DECIMAL_VALUE
    assert(parseTableIdentifier("a.123A") == TableIdentifier("123A", Some("a")))
    // ".123E3" should not be treated as token of type SCIENTIFIC_DECIMAL_VALUE
    assert(parseTableIdentifier("a.123E3_LIST") == TableIdentifier("123E3_LIST", Some("a")))
    // ".123D" should not be treated as token of type DOUBLE_LITERAL
    assert(parseTableIdentifier("a.123D_LIST") == TableIdentifier("123D_LIST", Some("a")))
    // ".123BD" should not be treated as token of type BIGDECIMAL_LITERAL
    assert(parseTableIdentifier("a.123BD_LIST") == TableIdentifier("123BD_LIST", Some("a")))
  }

  test("SPARK-17832 table identifier - contains backtick") {
    val complexName = TableIdentifier("`weird`table`name", Some("`d`b`1"))
    assert(complexName === parseTableIdentifier("```d``b``1`.```weird``table``name`"))
    assert(complexName === parseTableIdentifier(complexName.quotedString))
    intercept[ParseException](parseTableIdentifier(complexName.unquotedString))
    // Table identifier contains countious backticks should be treated correctly.
    val complexName2 = TableIdentifier("x``y", Some("d``b"))
    assert(complexName2 === parseTableIdentifier(complexName2.quotedString))
  }
}
