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

object QuotingUtils {
  private def quoteByDefault(elem: String): String = {
    "\"" + elem + "\""
  }

  def toSQLConf(conf: String): String = {
    quoteByDefault(conf)
  }

  def toSQLSchema(schema: String): String = {
    quoteByDefault(schema)
  }

  def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  def escapeSingleQuotedString(str: String): String = {
    val builder = new StringBuilder

    str.foreach {
      case '\'' => builder ++= s"\\\'"
      case ch => builder += ch
    }

    builder.toString()
  }
}
