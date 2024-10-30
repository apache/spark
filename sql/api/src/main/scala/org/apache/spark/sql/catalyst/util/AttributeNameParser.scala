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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.errors.DataTypeErrors

trait AttributeNameParser {

  /**
   * Used to split attribute name by dot with backticks rule. Backticks must appear in pairs, and
   * the quoted string must be a complete name part, which means `ab..c`e.f is not allowed. We can
   * use backtick only inside quoted name parts.
   */
  def parseAttributeName(name: String): Seq[String] = {
    def e = DataTypeErrors.attributeNameSyntaxError(name)
    val nameParts = scala.collection.mutable.ArrayBuffer.empty[String]
    val tmp = scala.collection.mutable.ArrayBuffer.empty[Char]
    var inBacktick = false
    var i = 0
    while (i < name.length) {
      val char = name(i)
      if (inBacktick) {
        if (char == '`') {
          if (i + 1 < name.length && name(i + 1) == '`') {
            tmp += '`'
            i += 1
          } else {
            inBacktick = false
            if (i + 1 < name.length && name(i + 1) != '.') throw e
          }
        } else {
          tmp += char
        }
      } else {
        if (char == '`') {
          if (tmp.nonEmpty) throw e
          inBacktick = true
        } else if (char == '.') {
          if (name(i - 1) == '.' || i == name.length - 1) throw e
          nameParts += tmp.mkString
          tmp.clear()
        } else {
          tmp += char
        }
      }
      i += 1
    }
    if (inBacktick) throw e
    nameParts += tmp.mkString
    nameParts.toSeq
  }
}

object AttributeNameParser extends AttributeNameParser
