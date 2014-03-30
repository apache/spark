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

import org.scalastyle.{PositionError, ScalariformChecker, ScalastyleError}
import scalariform.lexer.{MultiLineComment, ScalaDocComment, SingleLineComment, Token}
import scalariform.parser.CompilationUnit

class SparkSpaceAfterCommentStartChecker extends ScalariformChecker {
  val errorKey: String = "insert.a.single.space.after.comment.start.and.before.end"

  private def multiLineCommentRegex(comment: Token) =
    Pattern.compile( """/\*\S+.*""", Pattern.DOTALL).matcher(comment.text.trim).matches() ||
      Pattern.compile( """/\*.*\S\*/""", Pattern.DOTALL).matcher(comment.text.trim).matches()

  private def scalaDocPatternRegex(comment: Token) =
    Pattern.compile( """/\*\*\S+.*""", Pattern.DOTALL).matcher(comment.text.trim).matches() ||
      Pattern.compile( """/\*\*.*\S\*/""", Pattern.DOTALL).matcher(comment.text.trim).matches()

  private def singleLineCommentRegex(comment: Token): Boolean =
    comment.text.trim.matches( """//\S+.*""") && !comment.text.trim.matches( """///+""")

  override def verify(ast: CompilationUnit): List[ScalastyleError] = {
    ast.tokens
      .filter(hasComment)
      .map {
      _.associatedWhitespaceAndComments.comments.map {
        case x: SingleLineComment if singleLineCommentRegex(x.token) => Some(x.token.offset)
        case x: MultiLineComment if multiLineCommentRegex(x.token) => Some(x.token.offset)
        case x: ScalaDocComment if scalaDocPatternRegex(x.token) => Some(x.token.offset)
        case _ => None
      }.flatten
    }.flatten.map(PositionError(_))
  }


  private def hasComment(x: Token) =
    x.associatedWhitespaceAndComments != null && !x.associatedWhitespaceAndComments.comments.isEmpty

}
