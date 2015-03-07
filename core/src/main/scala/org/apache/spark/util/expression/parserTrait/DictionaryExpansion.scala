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
package org.apache.spark.util.expression.parserTrait

import org.apache.spark.util.expression.BaseParser
import Utils._

/**
 * Expands out dictionary expressions to their provided values
 */
private[spark] trait DictionaryExpansion extends BaseParser {
  /**
   * Dictionary of symbols - host class must provide.
   * Symbols are matched case-insensitive. Longer symbols are matched
   * matched in preference of longer ones
   */
  protected def dict: Map[String, Long]

  private def userKeyMatcher: Parser[String] = createListRegexp(dict.keys)

  private def dictExpressionExpansion: Parser[Double] = userKeyMatcher ^^ {
    case dictKeysRegex => dict(dictKeysRegex.toLowerCase)
  }

  protected abstract override def stackedExtensions: Parser[Double] = dictExpressionExpansion |
    super.stackedExtensions
}

