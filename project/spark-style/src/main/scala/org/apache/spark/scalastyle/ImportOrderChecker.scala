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

package org.apache.spark.scalastyle

import java.util.regex.Pattern

import scala.collection.mutable

import org.scalastyle.{PositionError, ScalariformChecker, ScalastyleError}
import scalariform.lexer._
import scalariform.parser._

/**
 * Style checker that enforces import ordering. The following rules are checked:
 *
 * - Imports are grouped according to the Spark coding style. This means the following groups:
 *  - java/javax
 *  - scala
 *  - non-Spark
 *  - org.apache.spark
 * - Within each group, imports are ordered alphabetically
 * - In multi-import statements, things are ordered alphabetically, with method / packages
 *   (assumed to be any string starting with a lower case letter) coming before classes.
 */
class ImportOrderChecker extends ScalariformChecker {
  val errorKey: String = "import.statement.order({0})"

  private val groups = Seq(
    ("java" -> Pattern.compile("javax?\\..+")),
    ("scala" -> Pattern.compile("scala\\..+")),
    ("3rdParty" -> Pattern.compile("(?!org\\.apache\\.spark\\.).*")),
    ("spark" -> Pattern.compile("org\\.apache\\.spark\\..*")))

  private var ast: AstNode = _
  private var lastImport: AstNode = _

  private var currentGroup = 0
  private var lastImportInGroup: String = _

  override def verify(ast: CompilationUnit): List[ScalastyleError] = {
    val CompilationUnit(statements, _) = ast

    this.ast = ast

    statements.immediateChildren.flatMap { n =>
      val result = n match {
        case ImportClause(_, Expr(contents), _) =>
          val text = exprToText(contents)
          checkImport(text, n.firstToken.offset)

        case ImportClause(_, BlockImportExpr(prefix, selectors), _) =>
          val text = exprToText(prefix.contents)
          checkImport(text, n.firstToken.offset) ++ checkSelectors(selectors)

        case _ =>
          Nil
      }
      lastImport = n
      result
    }
  }

  private def exprToText(contents: List[ExprElement]): String = {
    contents.flatMap {
      case GeneralTokens(toks) =>
        toks.map(_.text)

      case n =>
        throw new IllegalStateException(s"FIXME: unexpected expr child node $n")
    }.mkString("")
  }

  /**
   * Check that the given import belongs to the current group and is ordered correctly within it.
   */
  private def checkImport(str: String, offset: Int): Seq[ScalastyleError] = {
    val errors = new mutable.ListBuffer[ScalastyleError]()

    if (!groups(currentGroup)._2.matcher(str).matches()) {
      // If a statement doesn't match the current group, there are two options:
      // - It belongs to a previous group, in which case an error is flagged.
      // - It belongs to a following group, in which case the group index moves forward.
      for (i <- 0 to currentGroup - 1) {
        if (groups(i)._2.matcher(str).matches()) {
          return Seq(PositionError(offset,
            List(s"'$str' should be in group ${groups(i)._1}, not ${groups(currentGroup)._1}")))
        }
      }

      var nextGroup = currentGroup + 1
      while (nextGroup < groups.size && !groups(nextGroup)._2.matcher(str).matches()) {
        nextGroup += 1
      }

      if (nextGroup == groups.size) {
        throw new IllegalStateException(s"FIXME: import statement does not match any group: $str")
      }

      errors ++= checkGroupSeparation(currentGroup, nextGroup, offset)
      currentGroup = nextGroup
      lastImportInGroup = null
    } else {
      // If the statement is in the same group, make sure there is no empty line between it and
      // the previous import.
      errors ++= checkNoSeparator(offset)
    }

    // Ensure import is in alphabetical order.
    if (lastImportInGroup != null && compareImports(lastImportInGroup, str) > 0) {
      errors += PositionError(offset,
        List(s"'$str' is in wrong order relative to '$lastImportInGroup'."))
    }

    lastImportInGroup = str
    errors.toSeq
  }

  /**
   * Check that the imports inside a multi-import block are ordered.
   */
  private def checkSelectors(selectors: ImportSelectors): Seq[ScalastyleError] = {
    val ImportSelectors(_, first, others, _) = selectors

    val errors = new mutable.ListBuffer[ScalastyleError]()
    val names = Seq(first.contents.head.tokens.head.text) ++
      others.map( _._2.contents.head.tokens.head.text)

    var last: String = null
    for (name <- names) {
      if (last != null && compareNames(last, name, false) > 0) {
        errors += PositionError(selectors.firstToken.offset,
          List(s"$name should come before $last"))
      }
      last = name
    }

    errors.toSeq
  }

  /**
   * When the current import group changes, checks that there is a single empty line between
   * the last import statement in the previous group and the first statement in the new one.
   */
  private def checkGroupSeparation(
      lastGroup: Int,
      nextGroup: Int,
      nextGroupOffset: Int): Option[ScalastyleError] = {
    if (lastGroup != nextGroup && lastImport != null) {
      val start = lastImport.lastToken.offset + lastImport.lastToken.length
      if (countNewLines(start, nextGroupOffset) != 2) {
        val last = groups(lastGroup)._1
        val current = groups(nextGroup)._1
        return Some(PositionError(nextGroupOffset,
          List(s"There should be a single empty line separating groups '$last' and '$current'.")))
      }
    }

    None
  }

  /**
   * Check that there are no empty lines between imports in the same group.
   */
  private def checkNoSeparator(offset: Int): Option[ScalastyleError] = {
    if (lastImportInGroup != null) {
      val start = lastImport.lastToken.offset + lastImport.lastToken.length
      if (countNewLines(start, offset) != 1) {
          return Some(PositionError(offset,
            List(s"There should be no empty line separating imports in the same group.")))
      }
    }
    None
  }

  /**
   * Counts the number of new lines between the given offsets, adjusted for comments.
   */
  private def countNewLines(start: Int, end: Int): Int = {
    var count = 0
    ast.tokens.filter { t => t.offset >= start && t.offset < end }.foreach { t =>
      val commentsToken = t.associatedWhitespaceAndComments
      if (commentsToken != null) {
        var ignoreNext = false
        commentsToken.tokens.foreach {
          case c: MultiLineComment =>
            // Do not count a new line after a multi-line comment.
            ignoreNext = true
          case w: Whitespace =>
            if (!ignoreNext) {
              // Assumes "\n" only used for new lines.
              count += w.text.filter(_ == '\n').size
            }
            ignoreNext = true
          case _ =>
            // Nothing to do.
        }
      }
    }
    count
  }

  /**
   * Compares two import statements, comparing each component of the import separately.
   *
   * The import statements can end with a dangling `.`, meaning they're the start of a
   * multi-import block.
   */
  private def compareImports(imp1: String, imp2: String): Int = {
    val imp1Components = imp1.split("[.]")
    val imp2Components = imp2.split("[.]")
    val max = math.min(imp1Components.size, imp2Components.size)
    for (i <- 0 to (max - 1)) {
      val comp1 = imp1Components(i)
      val comp2 = imp2Components(i)
      val result = compareNames(comp1, comp2, true)
      if (result != 0) {
        return result
      }
    }

    // At this point, there is still a special case: where one import is a multi-import block
    // (and, thus, has no extra components) and another is a wildcard; the wildcard should come
    // first.
    val diff = imp1Components.size - imp2Components.size
    if (diff == -1 && imp1.endsWith(".") && imp2Components.last == "_") {
      1
    } else if (diff == 1 && imp2.endsWith(".") && imp1Components.last == "_") {
      -1
    } else {
      diff
    }
  }

  /**
   * Compares two strings that represent a single imported artifact; this considers lower-case
   * names as being "lower" than upper case ones.
   *
   * @param name1 First name.
   * @param name2 Second name.
   * @param isImport If true, orders names according to the import statement rules:
   *                 "_" should come before other names, and capital letters should come
   *                 before lower case ones. Otherwise, do the opposite, which are the ordering
   *                 rules for names within a multi-import block.
   */
  private def compareNames(name1: String, name2: String, isImport: Boolean): Int = {
    if (name1 != "_") {
      if (name2 == "_") {
        -1 * compareNames(name2, name1, isImport)
      } else {
        val isName1UpperCase = Character.isUpperCase(name1.codePointAt(0))
        val isName2UpperCase = Character.isUpperCase(name2.codePointAt(0))

        if (isName1UpperCase == isName2UpperCase) {
          name1.compareToIgnoreCase(name2)
        } else {
          if (isName1UpperCase && !isImport) 1 else -1
        }
      }
    } else {
      if (isImport) -1 else 1
    }
  }

}
