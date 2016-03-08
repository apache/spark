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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types._


/**
 * A collection of utility methods and patterns for parsing query texts.
 */
// TODO: merge with ParseUtils
object ParserUtils {

  object Token {
    def unapply(node: ASTNode): Some[(String, List[ASTNode])] = {
      CurrentOrigin.setPosition(node.line, node.positionInLine)
      node.pattern
    }
  }

  private val escapedIdentifier = "`(.+)`".r
  private val doubleQuotedString = "\"([^\"]+)\"".r
  private val singleQuotedString = "'([^']+)'".r

  // Token patterns
  val COUNT = "(?i)COUNT".r
  val SUM = "(?i)SUM".r
  val AND = "(?i)AND".r
  val OR = "(?i)OR".r
  val NOT = "(?i)NOT".r
  val TRUE = "(?i)TRUE".r
  val FALSE = "(?i)FALSE".r
  val LIKE = "(?i)LIKE".r
  val RLIKE = "(?i)RLIKE".r
  val REGEXP = "(?i)REGEXP".r
  val IN = "(?i)IN".r
  val DIV = "(?i)DIV".r
  val BETWEEN = "(?i)BETWEEN".r
  val WHEN = "(?i)WHEN".r
  val CASE = "(?i)CASE".r
  val INTEGRAL = "[+-]?\\d+".r
  val DECIMAL = "[+-]?((\\d+(\\.\\d*)?)|(\\.\\d+))".r

  /**
   * Strip quotes, if any, from the string.
   */
  def unquoteString(str: String): String = {
    str match {
      case singleQuotedString(s) => s
      case doubleQuotedString(s) => s
      case other => other
    }
  }

  /**
   * Strip backticks, if any, from the string.
   */
  def cleanIdentifier(ident: String): String = {
    ident match {
      case escapedIdentifier(i) => i
      case plainIdent => plainIdent
    }
  }

  def getClauses(
      clauseNames: Seq[String],
      nodeList: Seq[ASTNode]): Seq[Option[ASTNode]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.text.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) matches.tail else Nil)
      matches.headOption
    }

    if (remainingNodes.nonEmpty) {
      sys.error(
        s"""Unhandled clauses: ${remainingNodes.map(_.treeString).mkString("\n")}.
            |You are likely trying to use an unsupported Hive feature."""".stripMargin)
    }
    clauses
  }

  def getClause(clauseName: String, nodeList: Seq[ASTNode]): ASTNode = {
    getClauseOption(clauseName, nodeList).getOrElse(sys.error(
      s"Expected clause $clauseName missing from ${nodeList.map(_.treeString).mkString("\n")}"))
  }

  def getClauseOption(clauseName: String, nodeList: Seq[ASTNode]): Option[ASTNode] = {
    nodeList.filter { case ast: ASTNode => ast.text == clauseName } match {
      case Seq(oneMatch) => Some(oneMatch)
      case Seq() => None
      case _ => sys.error(s"Found multiple instances of clause $clauseName")
    }
  }

  def extractTableIdent(tableNameParts: ASTNode): TableIdentifier = {
    tableNameParts.children.map {
      case Token(part, Nil) => cleanIdentifier(part)
    } match {
      case Seq(tableOnly) => TableIdentifier(tableOnly)
      case Seq(databaseName, table) => TableIdentifier(table, Some(databaseName))
      case other => sys.error("Hive only supports tables names like 'tableName' " +
        s"or 'databaseName.tableName', found '$other'")
    }
  }

  def nodeToDataType(node: ASTNode): DataType = node match {
    case Token("TOK_DECIMAL", precision :: scale :: Nil) =>
      DecimalType(precision.text.toInt, scale.text.toInt)
    case Token("TOK_DECIMAL", precision :: Nil) =>
      DecimalType(precision.text.toInt, 0)
    case Token("TOK_DECIMAL", Nil) => DecimalType.USER_DEFAULT
    case Token("TOK_BIGINT", Nil) => LongType
    case Token("TOK_INT", Nil) => IntegerType
    case Token("TOK_TINYINT", Nil) => ByteType
    case Token("TOK_SMALLINT", Nil) => ShortType
    case Token("TOK_BOOLEAN", Nil) => BooleanType
    case Token("TOK_STRING", Nil) => StringType
    case Token("TOK_VARCHAR", Token(_, Nil) :: Nil) => StringType
    case Token("TOK_CHAR", Token(_, Nil) :: Nil) => StringType
    case Token("TOK_FLOAT", Nil) => FloatType
    case Token("TOK_DOUBLE", Nil) => DoubleType
    case Token("TOK_DATE", Nil) => DateType
    case Token("TOK_TIMESTAMP", Nil) => TimestampType
    case Token("TOK_BINARY", Nil) => BinaryType
    case Token("TOK_LIST", elementType :: Nil) => ArrayType(nodeToDataType(elementType))
    case Token("TOK_STRUCT", Token("TOK_TABCOLLIST", fields) :: Nil) =>
      StructType(fields.map(nodeToStructField))
    case Token("TOK_MAP", keyType :: valueType :: Nil) =>
      MapType(nodeToDataType(keyType), nodeToDataType(valueType))
    case _ =>
      noParseRule("DataType", node)
  }

  def nodeToStructField(node: ASTNode): StructField = node match {
    case Token("TOK_TABCOL", Token(fieldName, Nil) :: dataType :: Nil) =>
      StructField(cleanIdentifier(fieldName), nodeToDataType(dataType), nullable = true)
    case Token("TOK_TABCOL", Token(fieldName, Nil) :: dataType :: comment :: Nil) =>
      val meta = new MetadataBuilder().putString("comment", unquoteString(comment.text)).build()
      StructField(cleanIdentifier(fieldName), nodeToDataType(dataType), nullable = true, meta)
    case _ =>
      noParseRule("StructField", node)
  }

  /**
   * Throw an exception because we cannot parse the given node.
   */
  def noParseRule(msg: String, node: ASTNode): Nothing = {
    throw new NotImplementedError(
      s"[$msg]: No parse rules for ASTNode type: ${node.tokenType}, tree:\n${node.treeString}")
  }

}
