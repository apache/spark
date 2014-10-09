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

import org.scalastyle.{PositionError, ScalariformChecker, ScalastyleError}
import scala.collection.mutable.{ListBuffer, Queue}
import scalariform.lexer.{Token, Tokens}
import scalariform.lexer.Tokens._
import scalariform.parser.CompilationUnit

class SparkSpaceBeforeLeftBraceChecker extends ScalariformChecker {
  val errorKey: String = "insert.a.single.space.before.left.brace"

  val rememberQueue: Queue[Token] = Queue[Token]()

  // The list of disallowed tokens before left brace without single space.
  val disallowedTokensBeforeLBrace = Seq (
    ARROW, ELSE, OP, RPAREN, TRY, MATCH, NEW, DO, FINALLY, PACKAGE, RETURN, THROW, YIELD, VARID
  )

  override def verify(ast: CompilationUnit): List[ScalastyleError] = {

    var list: ListBuffer[ScalastyleError] = new ListBuffer[ScalastyleError]

    for (token <- ast.tokens) {
      rememberToken(token)
      if (isLBrace(token) &&
          isTokenAfterSpecificTokens(token) &&
          !hasSingleWhiteSpaceBefore(token)) {
        list += new PositionError(token.offset)
      }
    }
    list.toList
  }

  private def rememberToken(x: Token) = {
    rememberQueue.enqueue(x)
    if (rememberQueue.size > 2) {
      rememberQueue.dequeue
    }
    x
  }

  private def isTokenAfterSpecificTokens(x: Token) = {
    val previousToken = rememberQueue.head
    disallowedTokensBeforeLBrace.contains(previousToken.tokenType)
  }

  private def isLBrace(x: Token) =
    x.tokenType == Tokens.LBRACE

  private def hasSingleWhiteSpaceBefore(x: Token) =
    x.associatedWhitespaceAndComments.whitespaces.size == 1
}
