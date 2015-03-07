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

import scala.util.matching.Regex

/**
 * Utility class
 */
private[spark] object Utils {
  /**
   * Creates a regexp to match any of the passed list of keys
   * @param keys List of keys to match
   * @param caseInsensitive whether the match should be case insensitive
   * @return RegEx that will math any of the keys
   */
  private[spark] def createListRegexp(keys: Iterable[String], caseInsensitive: Boolean = true):
      Regex = {

    val regexChars: Set[Char] = """[\^$.|?*+()""".toList.toSet

    def escapeRegExChars(word: String): String = {
      word.toCharArray.map (c =>
        if (regexChars.contains(c)) s"\\$c" else s"$c"
      ).mkString
    }
    val regexStart: String = if (caseInsensitive) "(?i)(" else "("

    keys.map(escapeRegExChars).mkString(regexStart, "|", ")").r
  }
}

