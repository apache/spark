/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli.thrift

import org.apache.log4j.PatternLayout

object CLIServiceUtils {

  private val SEARCH_STRING_ESCAPE = '\\'
  val verboseLayout = new PatternLayout("%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n")
  val nonVerboseLayout = new PatternLayout("%-5p : %m%n")

  /**
   * Convert a SQL search pattern into an equivalent Java Regex.
   *
   * @param pattern input which may contain '%' or '_' wildcard characters, or
   *                these characters escaped using { @code getSearchStringEscape()}.
   * @return replace %/_ with regex search characters, also handle escaped
   *         characters.
   */
  def patternToRegex(pattern: String): String = {
    if (pattern == null) {
      ".*"
    } else {
      val result = new StringBuilder(pattern.length)
      var escaped = false
      var i = 0
      pattern.toCharArray.foreach(c => {
        if (escaped) {
          if (c != SEARCH_STRING_ESCAPE) {
            escaped = false
          }
          result.append(c)
        } else if (c == SEARCH_STRING_ESCAPE) {
          escaped = true
        } else if (c == '%') {
          result.append(".*")
        } else if (c == '_') {
          result.append('.')
        } else {
          result.append(Character.toLowerCase(c))
        }
      })
      result.toString()
    }
  }
}
