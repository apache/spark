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

import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}

import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.util.SparkParserUtils.withOrigin

/**
 * AST builder for extracting parameter markers from SQL parse trees.
 * This builder traverses the parse tree and collects all parameter literals
 * without building full Catalyst expressions.
 */
class SubstituteParmsAstBuilder extends SqlBaseParserBaseVisitor[AnyRef] {

  private val namedParams = scala.collection.mutable.Set[String]()
  private val positionalParams = scala.collection.mutable.ListBuffer[Int]()

  /**
   * Extract parameter information from a parse context.
   * This method traverses the entire parse tree to find parameter markers.
   */
  def extractFromContext(ctx: ParseTree): ParameterInfo = {
    // Clear previous state
    namedParams.clear()
    positionalParams.clear()

    // Visit the context to collect parameters
    visit(ctx)

    ParameterInfo(namedParams.toSet, positionalParams.toList)
  }

  /**
   * Create a named parameter which represents a literal with a non-bound value and unknown type.
   */
  override def visitNamedParameterLiteral(
      ctx: NamedParameterLiteralContext): String = withOrigin(ctx) {
    val paramName = ctx.identifier().getText
    namedParams += paramName
    paramName
  }

  /**
   * Create a positional parameter which represents a literal
   * with a non-bound value and unknown type.
   */
  override def visitPosParameterLiteral(
      ctx: PosParameterLiteralContext): String = withOrigin(ctx) {
    val startIndex = ctx.QUESTION().getSymbol.getStartIndex
    positionalParams += startIndex
    "?"
  }

  /**
   * Override visit to ensure we traverse all children to find parameters.
   */
  override def visit(tree: ParseTree): AnyRef = {
    if (tree == null) return null

    // Check if this is a parameter literal
    tree match {
      case ctx: NamedParameterLiteralContext =>
        visitNamedParameterLiteral(ctx)
      case ctx: PosParameterLiteralContext =>
        visitPosParameterLiteral(ctx)
      case ruleNode: RuleNode =>
        // Continue traversing children for rule nodes
        visitChildren(ruleNode)
      case _ =>
        // For other types (like terminal nodes), don't traverse children
        null
    }
  }

  /**
   * Visit all children of a node to find parameters.
   */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node == null) return null

    var result: AnyRef = null
    for (i <- 0 until node.getChildCount) {
      val child = node.getChild(i)
      if (child != null) {
        val childResult = visit(child)
        if (result == null) {
          result = childResult
        }
      }
    }
    result
  }

  /**
   * Visit terminal nodes (leaf nodes in the parse tree).
   */
  override def visitTerminal(node: TerminalNode): AnyRef = {
    // Terminal nodes don't contain parameters themselves,
    // but we need to handle them in the traversal
    null
  }
}

/**
 * Data class to hold information about extracted parameters.
 * This is defined here to make it available to the AST builder.
 */
case class ParameterInfo(
    namedParameters: Set[String],
    positionalParameters: List[Int]) {

  def isEmpty: Boolean = namedParameters.isEmpty && positionalParameters.isEmpty
  def nonEmpty: Boolean = !isEmpty
  def totalCount: Int = namedParameters.size + positionalParameters.size

  def merge(other: ParameterInfo): ParameterInfo = {
    ParameterInfo(
      namedParameters ++ other.namedParameters,
      positionalParameters ++ other.positionalParameters
    )
  }

  override def toString: String = {
    val named = if (namedParameters.nonEmpty) {
      s"named: ${namedParameters.mkString(", ")}"
    } else ""
    val positional = if (positionalParameters.nonEmpty) {
      s"positional: ${positionalParameters.size}"
    } else ""
    val parts = Seq(named, positional).filter(_.nonEmpty)
    if (parts.nonEmpty) s"ParameterInfo(${parts.mkString(", ")})" else "ParameterInfo(empty)"
  }
}
