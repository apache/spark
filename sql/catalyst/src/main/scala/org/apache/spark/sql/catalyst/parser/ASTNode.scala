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

import org.antlr.runtime.{Token, TokenRewriteStream}

import org.apache.spark.sql.catalyst.trees.{Origin, TreeNode}

case class ASTNode(
    token: Token,
    startIndex: Int,
    stopIndex: Int,
    children: List[ASTNode],
    stream: TokenRewriteStream) extends TreeNode[ASTNode] {
  /** Cache the number of children. */
  val numChildren: Int = children.size

  /** tuple used in pattern matching. */
  val pattern: Some[(String, List[ASTNode])] = Some((token.getText, children))

  /** Line in which the ASTNode starts. */
  lazy val line: Int = {
    val line = token.getLine
    if (line == 0) {
      if (children.nonEmpty) children.head.line
      else 0
    } else {
      line
    }
  }

  /** Position of the Character at which ASTNode starts. */
  lazy val positionInLine: Int = {
    val line = token.getCharPositionInLine
    if (line == -1) {
      if (children.nonEmpty) children.head.positionInLine
      else 0
    } else {
      line
    }
  }

  /** Origin of the ASTNode. */
  override val origin: Origin = Origin(Some(line), Some(positionInLine))

  /** Source text. */
  lazy val source: String = stream.toOriginalString(startIndex, stopIndex)

  /** Get the source text that remains after this token. */
  lazy val remainder: String = {
    stream.fill()
    stream.toOriginalString(stopIndex + 1, stream.size() - 1).trim()
  }

  def text: String = token.getText

  def tokenType: Int = token.getType

  /**
   * Checks if this node is equal to another node.
   *
   * Right now this function only checks the name, type, text and children of the node
   * for equality.
   */
  def treeEquals(other: ASTNode): Boolean = {
    def check(f: ASTNode => Any): Boolean = {
      val l = f(this)
      val r = f(other)
      (l == null && r == null) || l.equals(r)
    }
    if (other == null) {
      false
    } else if (!check(_.token.getType)
      || !check(_.token.getText)
      || !check(_.numChildren)) {
      false
    } else {
      children.zip(other.children).forall {
        case (l, r) => l treeEquals r
      }
    }
  }

  override def simpleString: String = s"$text $line, $startIndex, $stopIndex, $positionInLine "
}
