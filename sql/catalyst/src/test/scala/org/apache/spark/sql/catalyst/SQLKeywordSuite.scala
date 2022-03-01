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

package org.apache.spark.sql.catalyst

import java.io.File
import java.nio.file.Files

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.fileToString

trait SQLKeywordUtils extends SparkFunSuite with SQLHelper {

  val sqlSyntaxDefs = {
    val sqlBaseParserPath =
      getWorkspaceFilePath("sql", "catalyst", "src", "main", "antlr4", "org",
        "apache", "spark", "sql", "catalyst", "parser", "SqlBaseParser.g4").toFile

    val sqlBaseLexerPath =
      getWorkspaceFilePath("sql", "catalyst", "src", "main", "antlr4", "org",
        "apache", "spark", "sql", "catalyst", "parser", "SqlBaseLexer.g4").toFile

    (fileToString(sqlBaseParserPath) + fileToString(sqlBaseLexerPath)).split("\n")
  }

  // each element is an array of 4 string: the keyword name, reserve or not in Spark ANSI mode,
  // Spark default mode, and the SQL standard.
  val keywordsInDoc: Array[Array[String]] = {
    val docPath = {
      getWorkspaceFilePath("docs", "sql-ref-ansi-compliance.md").toFile
    }
    fileToString(docPath).split("\n")
      .dropWhile(!_.startsWith("|Keyword|")).drop(2).takeWhile(_.startsWith("|"))
      .map(_.stripPrefix("|").split("\\|").map(_.trim))
  }

  private def parseAntlrGrammars[T](startTag: String, endTag: String)
      (f: PartialFunction[String, Seq[T]]): Set[T] = {
    val keywords = new mutable.ArrayBuffer[T]
    val default = (_: String) => Nil
    var startTagFound = false
    var parseFinished = false
    val lineIter = sqlSyntaxDefs.iterator
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
    assert(keywords.nonEmpty && startTagFound && parseFinished,
      "cannot extract keywords from the `SqlBaseParser.g4` or `SqlBaseLexer.g4` file, " +
      s"so please check if the start/end tags (`$startTag` and `$endTag`) " +
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
          // Filters out inappropriate entries, e.g., `!` in `NOT: 'NOT' | '!';`
          val litDef = """([A-Z_]+)""".r
          val literals = splitDefs.map(_.replaceAll("'", "").trim).toSeq.flatMap {
            case litDef(lit) => Some(lit)
            case _ => None
          }
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
  val allCandidateKeywords: Set[String] = {
    val kwDef = """([A-Z_]+):.+;""".r
    parseAntlrGrammars(
        "//--SPARK-KEYWORD-LIST-START", "//--SPARK-KEYWORD-LIST-END") {
      // Parses a pattern, e.g., `AFTER: 'AFTER';`
      case kwDef(symbol) =>
        if (symbolsToExpandIntoDifferentLiterals.contains(symbol)) {
          symbolsToExpandIntoDifferentLiterals(symbol)
        } else {
          symbol :: Nil
        }
    }
  }

  val nonReservedKeywordsInAnsiMode: Set[String] = {
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

  val nonReservedKeywordsInDefaultMode: Set[String] = {
    val kwDef = """\s*[\|:]\s*([A-Z_]+)\s*""".r
    parseAntlrGrammars("//--DEFAULT-NON-RESERVED-START", "//--DEFAULT-NON-RESERVED-END") {
      // Parses a pattern, e.g., `    | AFTER`
      case kwDef(symbol) =>
        if (symbolsToExpandIntoDifferentLiterals.contains(symbol)) {
          symbolsToExpandIntoDifferentLiterals(symbol)
        } else {
          symbol :: Nil
        }
    }
  }
}

class SQLKeywordSuite extends SQLKeywordUtils {
  test("all keywords are documented") {
    val documentedKeywords = keywordsInDoc.map(_.head).toSet
    if (allCandidateKeywords != documentedKeywords) {
      val undocumented = (allCandidateKeywords -- documentedKeywords).toSeq.sorted
      fail("Some keywords are not documented: " + undocumented.mkString(", "))
    }
  }

  test("Spark keywords are documented correctly under ANSI mode") {
    // keywords under ANSI mode should either be reserved or non-reserved.
    keywordsInDoc.map(_.apply(1)).foreach { desc =>
      assert(desc == "reserved" || desc == "non-reserved")
    }

    val nonReservedInDoc = keywordsInDoc.filter(_.apply(1) == "non-reserved").map(_.head).toSet
    if (nonReservedKeywordsInAnsiMode != nonReservedInDoc) {
      val misImplemented = ((nonReservedInDoc -- nonReservedKeywordsInAnsiMode) ++
        (nonReservedKeywordsInAnsiMode -- nonReservedInDoc)).toSeq.sorted
      fail("Some keywords are documented and implemented inconsistently: " +
        misImplemented.mkString(", "))
    }
  }

  test("Spark keywords are documented correctly under default mode") {
    // keywords under default mode should either be strict-non-reserved or non-reserved.
    keywordsInDoc.map(_.apply(2)).foreach { desc =>
      assert(desc == "strict-non-reserved" || desc == "non-reserved")
    }

    val nonReservedInDoc = keywordsInDoc.filter(_.apply(2) == "non-reserved").map(_.head).toSet
    if (nonReservedKeywordsInDefaultMode != nonReservedInDoc) {
      val misImplemented = ((nonReservedInDoc -- nonReservedKeywordsInDefaultMode) ++
        (nonReservedKeywordsInDefaultMode -- nonReservedInDoc)).toSeq.sorted
      fail("Some keywords are documented and implemented inconsistently: " +
        misImplemented.mkString(", "))
    }
  }

  test("SQL 2016 keywords are documented correctly") {
    withTempDir { dir =>
      val tmpFile = new File(dir, "tmp")
      val is = Thread.currentThread().getContextClassLoader
        .getResourceAsStream("ansi-sql-2016-reserved-keywords.txt")
      Files.copy(is, tmpFile.toPath)
      val reservedKeywordsInSql2016 = Files.readAllLines(tmpFile.toPath)
        .asScala.filterNot(_.startsWith("--")).map(_.trim).toSet
      val documented = keywordsInDoc.filter(_.last == "reserved").map(_.head).toSet
      assert((documented -- reservedKeywordsInSql2016).isEmpty)
    }
  }
}
