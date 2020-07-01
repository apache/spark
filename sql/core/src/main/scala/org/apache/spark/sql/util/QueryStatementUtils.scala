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

package org.apache.spark.sql.util

import java.util.{ArrayList => JArrayList, List => JList}

/**
 * Util functions for handling Query statements.
 */
object QueryStatementUtils {

  // Adapted splitSemiColon from Hive 2.3's CliDriver.splitSemiColon.
  // Note: [SPARK-31595] if there is a `'` in a double quoted string, or a `"` in a single quoted
  // string, the origin implementation from Hive will not drop the trailing semicolon as expected,
  // hence we refined this function a little bit.
  def splitSemiColon(line: String): JList[String] = {
    var insideSingleQuote = false
    var insideDoubleQuote = false
    var insideComment = false
    var escape = false
    var beginIndex = 0
    val ret = new JArrayList[String]

    for (index <- 0 until line.length) {
      if (line.charAt(index) == '\'' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideDoubleQuote) {
          // flip the boolean variable
          insideSingleQuote = !insideSingleQuote
        }
      } else if (line.charAt(index) == '\"' && !insideComment) {
        // take a look to see if it is escaped
        // See the comment above about SPARK-31595
        if (!escape && !insideSingleQuote) {
          // flip the boolean variable
          insideDoubleQuote = !insideDoubleQuote
        }
      } else if (line.charAt(index) == '-') {
        val hasNext = index + 1 < line.length
        if (insideDoubleQuote || insideSingleQuote || insideComment) {
          // Ignores '-' in any case of quotes or comment.
          // Avoids to start a comment(--) within a quoted segment or already in a comment.
          // Sample query: select "quoted value --"
          //                                    ^^ avoids starting a comment if it's inside quotes.
        } else if (hasNext && line.charAt(index + 1) == '-') {
          // ignore quotes and ;
          insideComment = true
        }
      } else if (line.charAt(index) == ';') {
        if (insideSingleQuote || insideDoubleQuote || insideComment) {
          // do not split
        } else {
          // split, do not include ; itself
          ret.add(line.substring(beginIndex, index))
          beginIndex = index + 1
        }
      } else if (line.charAt(index) == '\n') {
        // with a new line the inline comment should end.
        if (!escape) {
          insideComment = false
        }
      }
      // set the escape
      if (escape) {
        escape = false
      } else if (line.charAt(index) == '\\') {
        escape = true
      }
    }
    ret.add(line.substring(beginIndex))
    ret
  }
}
