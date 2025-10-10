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
      val locations = namedParamLocations.getOrElseUpdate(
        paramName,
        scala.collection.mutable.ListBuffer[ParameterLocation]())
      locations += ParameterLocation(startIndex, stopIndex)

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

      // Question mark is single character, so stopIndex = startIndex + 1
      val stopIndex = startIndex + 1
      positionalParamLocations += ParameterLocation(startIndex, stopIndex)

      null // Return value not used
    }

  /**
   * Collect information about named parameter markers. This handles the namedParameterMarker case
   * in the shared parameterMarker grammar rule. Note: The return value is not used; this method
   * operates via side effects.
   */
  override def visitNamedParameterMarkerRule(ctx: NamedParameterMarkerRuleContext): AnyRef =
    withOrigin(ctx) {
      val paramName = ctx.namedParameterMarker().identifier().getText
      namedParams += paramName

      // Calculate the location of the entire parameter (including the colon)
      val startIndex = ctx.getStart.getStartIndex
      val stopIndex = ctx.getStop.getStopIndex + 1
      val locations = namedParamLocations.getOrElseUpdate(
        paramName,
        scala.collection.mutable.ListBuffer[ParameterLocation]())
      locations += ParameterLocation(startIndex, stopIndex)

      null // Return value not used
    }

  /**
   * Collect information about positional parameter markers. This handles the QUESTION case
   * in the shared parameterMarker grammar rule. Note: The return value is not used; this method
   * operates via side effects.
   */
  override def visitPositionalParameterMarkerRule(
      ctx: PositionalParameterMarkerRuleContext): AnyRef =
    withOrigin(ctx) {
      val paramIndex = positionalParams.size
      positionalParams += paramIndex

      // Parameter marker is single character, so stopIndex = startIndex + 1
      val startIndex = ctx.getStart.getStartIndex
      val stopIndex = startIndex + 1
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
      case ctx: ParameterStringValueContext =>
        // Handle parameter markers in string contexts
        visit(ctx.parameterMarker())
      case ctx: ParameterIntegerValueContext =>
        // Handle parameter markers in integer contexts
        visit(ctx.parameterMarker())
      case ctx: NamedParameterMarkerRuleContext =>
        visitNamedParameterMarkerRule(ctx)
      case ctx: PositionalParameterMarkerRuleContext =>
        visitPositionalParameterMarkerRule(ctx)
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
