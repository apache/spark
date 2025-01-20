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

import java.util.regex.Pattern

import org.apache.spark.sql.connector.catalog.Identifier

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

  def quoteIdentifier(name: String): String = {
    // Escapes back-ticks within the identifier name with double-back-ticks, and then quote the
    // identifier with back-ticks.
    "`" + name.replace("`", "``") + "`"
  }

  def quoteNameParts(name: Seq[String]): String = {
    name.map(part => quoteIdentifier(part)).mkString(".")
  }

  private val validIdentPattern = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*")

  def needQuote(part: String): Boolean = {
    !validIdentPattern.matcher(part).matches()
  }

  def quoteIfNeeded(part: String): String = {
    if (needQuote(part)) quoteIdentifier(part) else part
  }

  def quoted(namespace: Array[String]): String = {
    namespace.map(quoteIfNeeded).mkString(".")
  }

  def quoted(ident: Identifier): String = {
    if (ident.namespace.nonEmpty) {
      ident.namespace.map(quoteIfNeeded).mkString(".") + "." + quoteIfNeeded(ident.name)
    } else {
      quoteIfNeeded(ident.name)
    }
  }

  def fullyQuoted(ident: Identifier): String = {
    if (ident.namespace.nonEmpty) {
      ident.namespace.map(quoteIdentifier).mkString(".") + "." + quoteIdentifier(ident.name)
    } else {
      quoteIdentifier(ident.name)
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
