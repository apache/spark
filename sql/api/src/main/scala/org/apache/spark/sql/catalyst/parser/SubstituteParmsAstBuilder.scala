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
 * AST builder for extracting parameter markers and their locations from SQL parse trees. This
 * builder traverses the parse tree and collects parameter information for substitution.
 */
class SubstituteParmsAstBuilder extends SqlBaseParserBaseVisitor[AnyRef] {

  private val namedParams = scala.collection.mutable.Set[String]()
  private val positionalParams = scala.collection.mutable.ListBuffer[Int]()
  private val namedParamLocations =
    scala.collection.mutable.Map[String, scala.collection.mutable.ListBuffer[ParameterLocation]]()
  private val positionalParamLocations = scala.collection.mutable.ListBuffer[ParameterLocation]()

  /**
   * Extract parameter location information from a parse context. This method traverses the parse
   * tree and collects parameter locations for substitution.
   */
  def extractParameterLocations(ctx: ParseTree): ParameterLocationInfo = {
    // Clear previous state
    namedParams.clear()
    positionalParams.clear()
    namedParamLocations.clear()
    positionalParamLocations.clear()

    // Visit the context to collect parameters and their locations
    visit(ctx)

    ParameterLocationInfo(
      namedParamLocations.view.mapValues(_.toList).toMap,
      positionalParamLocations.toList)
  }

  /**
   * Collect information about a named parameter in a literal context. Note: The return value is
   * not used; this method operates via side effects.
   */
  override def visitNamedParameterLiteral(ctx: NamedParameterLiteralContext): AnyRef =
    withOrigin(ctx) {
      val paramName = ctx.namedParameterMarker().identifier().getText
      namedParams += paramName

      // Calculate the location of the entire parameter (including the colon)
      val startIndex = ctx.getStart.getStartIndex
      val stopIndex = ctx.getStop.getStopIndex + 1
      namedParamLocations.getOrElseUpdate(
        paramName,
        scala.collection.mutable.ListBuffer[ParameterLocation]()) += ParameterLocation(
        startIndex,
        stopIndex)

      null // Return value not used
    }

  /**
   * Collect information about a positional parameter in a literal context. Note: The return value
   * is not used; this method operates via side effects.
   */
  override def visitPosParameterLiteral(ctx: PosParameterLiteralContext): AnyRef =
    withOrigin(ctx) {
      val startIndex = ctx.QUESTION().getSymbol.getStartIndex
      positionalParams += startIndex

      // Calculate the location of the question mark
      val stopIndex = ctx.QUESTION().getSymbol.getStopIndex + 1
      positionalParamLocations += ParameterLocation(startIndex, stopIndex)

      null // Return value not used
    }

  /**
   * Collect information about named parameter markers in string literal contexts. This handles
   * the namedParameterMarker case added to the stringLit grammar rule. Note: The return value is
   * not used; this method operates via side effects.
   */
  override def visitNamedParameterValue(ctx: NamedParameterValueContext): AnyRef =
    withOrigin(ctx) {
      val paramName = ctx.namedParameterMarker().identifier().getText
      namedParams += paramName

      // Calculate the location of the entire parameter (including the colon)
      val startIndex = ctx.getStart.getStartIndex
      val stopIndex = ctx.getStop.getStopIndex + 1
      namedParamLocations.getOrElseUpdate(
        paramName,
        scala.collection.mutable.ListBuffer[ParameterLocation]()) += ParameterLocation(
        startIndex,
        stopIndex)

      null // Return value not used
    }

  /**
   * Collect information about named parameter markers in integer value contexts. This handles the
   * namedParameterMarker case added to the integerValue grammar rule. Note: The return value is
   * not used; this method operates via side effects.
   */
  override def visitNamedParameterIntegerValue(ctx: NamedParameterIntegerValueContext): AnyRef =
    withOrigin(ctx) {
      val paramName = ctx.namedParameterMarker().identifier().getText
      namedParams += paramName

      // Calculate the location of the entire parameter (including the colon)
      val startIndex = ctx.getStart.getStartIndex
      val stopIndex = ctx.getStop.getStopIndex + 1
      namedParamLocations.getOrElseUpdate(
        paramName,
        scala.collection.mutable.ListBuffer[ParameterLocation]()) += ParameterLocation(
        startIndex,
        stopIndex)

      null // Return value not used
    }

  /**
   * Collect information about positional parameter markers in integer value contexts. This
   * handles the QUESTION case added to the integerValue grammar rule (e.g., VARCHAR(?)). Note:
   * The return value is not used; this method operates via side effects.
   */
  override def visitPositionalParameterIntegerValue(
      ctx: PositionalParameterIntegerValueContext): AnyRef = withOrigin(ctx) {
    val paramIndex = positionalParams.size
    positionalParams += paramIndex

    // Calculate the location of the parameter marker
    val startIndex = ctx.getStart.getStartIndex
    val stopIndex = ctx.getStop.getStopIndex + 1
    positionalParamLocations += ParameterLocation(startIndex, stopIndex)

    null // Return value not used
  }

  /**
   * Collect information about positional parameter markers in string literal contexts. Note: The
   * return value is not used; this method operates via side effects.
   */
  override def visitPositionalParameterValue(ctx: PositionalParameterValueContext): AnyRef =
    withOrigin(ctx) {
      val paramIndex = positionalParams.size
      positionalParams += paramIndex

      // Calculate the location of the parameter marker
      val startIndex = ctx.getStart.getStartIndex
      val stopIndex = ctx.getStop.getStopIndex + 1
      positionalParamLocations += ParameterLocation(startIndex, stopIndex)

      null // Return value not used
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
      case ctx: NamedParameterValueContext =>
        visitNamedParameterValue(ctx)
      case ctx: NamedParameterIntegerValueContext =>
        visitNamedParameterIntegerValue(ctx)
      case ctx: PositionalParameterIntegerValueContext =>
        visitPositionalParameterIntegerValue(ctx)
      case ctx: PositionalParameterValueContext =>
        visitPositionalParameterValue(ctx)
      case ctx: StringLiteralInContextContext =>
        // For string literals in context, continue traversing to find any nested parameters
        visitChildren(ctx)
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
 * Data class to hold information about extracted parameters. This is defined here to make it
 * available to the AST builder.
 */
case class ParameterInfo(namedParameters: Set[String], positionalParameters: List[Int]) {

  def isEmpty: Boolean = namedParameters.isEmpty && positionalParameters.isEmpty
  def nonEmpty: Boolean = !isEmpty
  def totalCount: Int = namedParameters.size + positionalParameters.size

  def merge(other: ParameterInfo): ParameterInfo = {
    ParameterInfo(
      namedParameters ++ other.namedParameters,
      positionalParameters ++ other.positionalParameters)
  }

}

/**
 * Data class to hold parameter location information for substitution.
 */
case class ParameterLocationInfo(
    namedParameterLocations: Map[String, List[ParameterLocation]],
    positionalParameterLocations: List[ParameterLocation]) {

  def isEmpty: Boolean = namedParameterLocations.isEmpty && positionalParameterLocations.isEmpty
  def nonEmpty: Boolean = !isEmpty
  def totalCount: Int =
    namedParameterLocations.values.map(_.size).sum + positionalParameterLocations.size
}

/**
 * Data class representing a parameter location in the source text.
 */
case class ParameterLocation(start: Int, end: Int)
