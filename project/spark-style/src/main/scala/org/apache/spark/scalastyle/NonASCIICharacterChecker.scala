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

import scalariform.lexer.Token
import scalariform.parser.CompilationUnit

class NonASCIICharacterChecker extends ScalariformChecker {
  val errorKey: String = "non.ascii.character.disallowed"

  override def verify(ast: CompilationUnit): List[ScalastyleError] = {
    ast.tokens.filter(hasNonAsciiChars).map(x => PositionError(x.offset)).toList
  }

  private def hasNonAsciiChars(x: Token) =
    x.rawText.trim.nonEmpty && !Pattern.compile( """\p{ASCII}+""", Pattern.DOTALL)
    .matcher(x.text.trim).matches()

}
