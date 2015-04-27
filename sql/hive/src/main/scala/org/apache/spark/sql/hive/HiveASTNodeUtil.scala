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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.ql.parse.{ParseDriver, ParseUtils, ASTNode}
import org.apache.hadoop.hive.ql.Context
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.lib.Node

import org.apache.spark.sql.catalyst.trees.CurrentOrigin

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] object HiveASTNodeUtil {
  val nativeCommands = Seq(
    "TOK_ALTERDATABASE_OWNER",
    "TOK_ALTERDATABASE_PROPERTIES",
    "TOK_ALTERINDEX_PROPERTIES",
    "TOK_ALTERINDEX_REBUILD",
    "TOK_ALTERTABLE_ADDCOLS",
    "TOK_ALTERTABLE_ADDPARTS",
    "TOK_ALTERTABLE_ALTERPARTS",
    "TOK_ALTERTABLE_ARCHIVE",
    "TOK_ALTERTABLE_CLUSTER_SORT",
    "TOK_ALTERTABLE_DROPPARTS",
    "TOK_ALTERTABLE_PARTITION",
    "TOK_ALTERTABLE_PROPERTIES",
    "TOK_ALTERTABLE_RENAME",
    "TOK_ALTERTABLE_RENAMECOL",
    "TOK_ALTERTABLE_REPLACECOLS",
    "TOK_ALTERTABLE_SKEWED",
    "TOK_ALTERTABLE_TOUCH",
    "TOK_ALTERTABLE_UNARCHIVE",
    "TOK_ALTERVIEW_ADDPARTS",
    "TOK_ALTERVIEW_AS",
    "TOK_ALTERVIEW_DROPPARTS",
    "TOK_ALTERVIEW_PROPERTIES",
    "TOK_ALTERVIEW_RENAME",

    "TOK_CREATEDATABASE",
    "TOK_CREATEFUNCTION",
    "TOK_CREATEINDEX",
    "TOK_CREATEROLE",
    "TOK_CREATEVIEW",

    "TOK_DESCDATABASE",
    "TOK_DESCFUNCTION",

    "TOK_DROPDATABASE",
    "TOK_DROPFUNCTION",
    "TOK_DROPINDEX",
    "TOK_DROPROLE",
    "TOK_DROPTABLE_PROPERTIES",
    "TOK_DROPVIEW",
    "TOK_DROPVIEW_PROPERTIES",

    "TOK_EXPORT",

    "TOK_GRANT",
    "TOK_GRANT_ROLE",

    "TOK_IMPORT",

    "TOK_LOAD",

    "TOK_LOCKTABLE",

    "TOK_MSCK",

    "TOK_REVOKE",

    "TOK_SHOW_COMPACTIONS",
    "TOK_SHOW_CREATETABLE",
    "TOK_SHOW_GRANT",
    "TOK_SHOW_ROLE_GRANT",
    "TOK_SHOW_ROLE_PRINCIPALS",
    "TOK_SHOW_ROLES",
    "TOK_SHOW_SET_ROLE",
    "TOK_SHOW_TABLESTATUS",
    "TOK_SHOW_TBLPROPERTIES",
    "TOK_SHOW_TRANSACTIONS",
    "TOK_SHOWCOLUMNS",
    "TOK_SHOWDATABASES",
    "TOK_SHOWFUNCTIONS",
    "TOK_SHOWINDEXES",
    "TOK_SHOWLOCKS",
    "TOK_SHOWPARTITIONS",

    "TOK_SWITCHDATABASE",

    "TOK_UNLOCKTABLE"
  )

  // Commands that we do not need to explain.
  val noExplainCommands = Seq(
    "TOK_DESCTABLE",
    "TOK_SHOWTABLES",
    "TOK_TRUNCATETABLE"     // truncate table" is a NativeCommand, does not need to explain.
  ) ++ nativeCommands

  /**
   * A set of implicit transformations that allow Hive ASTNodes to be rewritten by transformations
   * similar to [[catalyst.trees.TreeNode]].
   *
   * Note that this should be considered very experimental and is not indented as a replacement
   * for TreeNode.  Primarily it should be noted ASTNodes are not immutable and do not appear to
   * have clean copy semantics.  Therefore, users of this class should take care when
   * copying/modifying trees that might be used elsewhere.
   */
  implicit class TransformableNode(n: ASTNode) {
    /**
     * Returns a copy of this node where `rule` has been recursively applied to it and all of its
     * children.  When `rule` does not apply to a given node it is left unchanged.
     * @param rule the function use to transform this nodes children
     */
    def transform(rule: PartialFunction[ASTNode, ASTNode]): ASTNode = {
      try {
        val afterRule = rule.applyOrElse(n, identity[ASTNode])
        afterRule.withChildren(
          nilIfEmpty(afterRule.getChildren)
            .asInstanceOf[Seq[ASTNode]]
            .map(ast => Option(ast).map(_.transform(rule)).orNull))
      } catch {
        case e: Exception =>
          println(dumpTree(n))
          throw e
      }
    }

    /**
     * Returns a scala.Seq equivalent to [s] or Nil if [s] is null.
     */
    private def nilIfEmpty[A](s: java.util.List[A]): Seq[A] =
      Option(s).map(_.toSeq).getOrElse(Nil)

    /**
     * Returns this ASTNode with the text changed to `newText`.
     */
    def withText(newText: String): ASTNode = {
      n.token.asInstanceOf[org.antlr.runtime.CommonToken].setText(newText)
      n
    }

    /**
     * Returns this ASTNode with the children changed to `newChildren`.
     */
    def withChildren(newChildren: Seq[ASTNode]): ASTNode = {
      (1 to n.getChildCount).foreach(_ => n.deleteChild(0))
      n.addChildren(newChildren)
      n
    }

    /**
     * Throws an error if this is not equal to other.
     *
     * Right now this function only checks the name, type, text and children of the node
     * for equality.
     */
    def checkEquals(other: ASTNode): Unit = {
      def check(field: String, f: ASTNode => Any): Unit = if (f(n) != f(other)) {
        sys.error(s"$field does not match for trees. " +
          s"'${f(n)}' != '${f(other)}' left: ${dumpTree(n)}, right: ${dumpTree(other)}")
      }
      check("name", _.getName)
      check("type", _.getType)
      check("text", _.getText)
      check("numChildren", n => nilIfEmpty(n.getChildren).size)

      val leftChildren = nilIfEmpty(n.getChildren).asInstanceOf[Seq[ASTNode]]
      val rightChildren = nilIfEmpty(other.getChildren).asInstanceOf[Seq[ASTNode]]
      leftChildren zip rightChildren foreach {
        case (l, r) => l checkEquals r
      }
    }
  }

  /** Extractor for matching Hive's AST Tokens. */
  object Token {
    /** @return matches of the form (tokenName, children). */
    def unapply(t: Any): Option[(String, Seq[ASTNode])] = t match {
      case t: ASTNode =>
        CurrentOrigin.setPosition(t.getLine, t.getCharPositionInLine)
        Some((t.getText,
          Option(t.getChildren).map(_.toList).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]))
      case _ => None
    }
  }

  val escapedIdentifier = "`([^`]+)`".r
  /** Strips backticks from ident if present */
  def cleanIdentifier(ident: String): String = ident match {
    case escapedIdentifier(i) => i
    case plainIdent => plainIdent
  }

  /**
   * Returns the AST for the given SQL string.
   */
  def getAst(sql: String): ASTNode = {
    /*
     * Context has to be passed in hive0.13.1.
     * Otherwise, there will be Null pointer exception,
     * when retrieving properties form HiveConf.
     */
    val hContext = new Context(new HiveConf())
    val node = ParseUtils.findRootNonNullToken((new ParseDriver).parse(sql, hContext))
    hContext.clear()
    node
  }

  /** todo: add comment here*/
  def getClauses(clauseNames: Seq[String], nodeList: Seq[ASTNode]): Seq[Option[Node]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.getText.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) matches.tail else Nil)
      matches.headOption
    }

    if (remainingNodes.nonEmpty) {
      sys.error(
        s"""Unhandled clauses: ${remainingNodes.map(dumpTree(_)).mkString("\n")}.
           |You are likely trying to use an unsupported Hive feature."""".stripMargin)
    }
    clauses
  }

  def getClause(clauseName: String, nodeList: Seq[Node]): Node =
    getClauseOption(clauseName, nodeList).getOrElse(sys.error(
      s"Expected clause $clauseName missing from ${nodeList.map(dumpTree(_)).mkString("\n")}"))

  def getClauseOption(clauseName: String, nodeList: Seq[Node]): Option[Node] = {
    nodeList.filter { case ast: ASTNode => ast.getText == clauseName } match {
      case Seq(oneMatch) => Some(oneMatch)
      case Seq() => None
      case _ => sys.error(s"Found multiple instances of clause $clauseName")
    }
  }


  def extractDbNameTableName(tableNameParts: Node): (Option[String], String) = {
    val (db, tableName) =
      tableNameParts.getChildren.map { case Token(part, Nil) => cleanIdentifier(part) } match {
        case Seq(tableOnly) => (None, tableOnly)
        case Seq(databaseName, table) => (Some(databaseName), table)
      }

    (db, tableName)
  }

  def extractTableIdent(tableNameParts: Node): Seq[String] = {
    tableNameParts.getChildren.map { case Token(part, Nil) => cleanIdentifier(part) } match {
      case Seq(tableOnly) => Seq(tableOnly)
      case Seq(databaseName, table) => Seq(databaseName, table)
      case other => sys.error("Hive only supports tables names like 'tableName' " +
        s"or 'databaseName.tableName', found '$other'")
    }
  }

  def dumpTree(
      node: Node,
      builder: StringBuilder = new StringBuilder,
      indent: Int = 0): StringBuilder = {
    node match {
      case a: ASTNode => builder.append(
        ("  " * indent) + a.getText + " " +
          a.getLine + ", " +
          a.getTokenStartIndex + "," +
          a.getTokenStopIndex + ", " +
          a.getCharPositionInLine + "\n")
      case other => sys.error(s"Non ASTNode encountered: $other")
    }

    Option(node.getChildren).map(_.toList).getOrElse(Nil).foreach(dumpTree(_, builder, indent + 1))
    builder
  }
}
