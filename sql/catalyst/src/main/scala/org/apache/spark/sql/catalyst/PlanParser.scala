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

import org.apache.spark.sql.catalyst.parser.ASTNode
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin

trait ParserBase {
  object Token {
    def unapply(node: ASTNode): Some[(String, List[ASTNode])] = {
      CurrentOrigin.setPosition(node.line, node.positionInLine)
      node.pattern
    }
  }

  protected val escapedIdentifier = "`(.+)`".r
  protected val doubleQuotedString = "\"([^\"]+)\"".r
  protected val singleQuotedString = "'([^']+)'".r

  protected def unquoteString(str: String) = str match {
    case singleQuotedString(s) => s
    case doubleQuotedString(s) => s
    case other => other
  }

  /** Strips backticks from ident if present */
  protected def cleanIdentifier(ident: String): String = ident match {
    case escapedIdentifier(i) => i
    case plainIdent => plainIdent
  }
}

/**
 * Abstract class for a parser that parses ASTNode to LogicalPlan.
 */
abstract class PlanParser extends PartialFunction[ASTNode, LogicalPlan] with ParserBase

