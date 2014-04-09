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

import scalariform.parser.CompilationUnit
import scalariform.lexer.{TokenType, Token, Tokens}
import org.scalastyle._

trait SpaceAroundTokenChecker extends ScalariformChecker {
  val DefaultTokens: String
  val disallowSpace: Boolean
  val beforeToken: Boolean

  def verify(ast: CompilationUnit) = {
    val tokens: Seq[TokenType] = getString("tokens", DefaultTokens).split(",").map(x => TokenType(x.trim))
    def checkSpaces(left: Token, middle: Token, right: Token) =
      if (beforeToken) charsBetweenTokens(left, middle) != (if (disallowSpace) 0 else 1)
      else charsBetweenTokens(middle, right) != (if (disallowSpace) 0 else 1)
    (for {
      l@List(left, middle, right) <- ast.tokens.sliding(3)
      if (l.forall(x => x.tokenType != Tokens.NEWLINE && x.tokenType != Tokens.NEWLINES)
        && tokens.contains(middle.tokenType)
        && !middle.associatedWhitespaceAndComments.containsNewline
        && !right.associatedWhitespaceAndComments.containsNewline
        && checkSpaces(left, middle, right))
    } yield {
      PositionError(middle.offset)
    }).toList
  }

}

class EnsureSpaceAfterToken extends SpaceAroundTokenChecker {
  val errorKey: String = "ensure.single.space.after.token"
  val DefaultTokens = "COLON, IF"
  val disallowSpace = false
  val beforeToken = false
}

class EnsureSpaceBeforeToken extends SpaceAroundTokenChecker {
  val errorKey: String = "ensure.single.space.before.token"
  val DefaultTokens = ""
  val disallowSpace = false
  val beforeToken = true
}

class DisallowSpaceBeforeToken extends SpaceAroundTokenChecker {
  val errorKey: String = "disallow.space.before.token"
  val DefaultTokens = "COLON, COMMA, RPAREN"
  val disallowSpace = true
  val beforeToken = true
}

class DisallowSpaceAfterToken extends SpaceAroundTokenChecker {
  val errorKey: String = "disallow.space.after.token"
  val DefaultTokens = "LPAREN"
  val disallowSpace = true
  val beforeToken = false
}