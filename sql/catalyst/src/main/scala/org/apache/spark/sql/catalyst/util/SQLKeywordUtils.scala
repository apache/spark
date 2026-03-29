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

import scala.util.Try

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.parser.{CatalystSqlParser, SqlBaseLexer}

private[sql] object SQLKeywordUtils extends SQLConfHelper {

  final private val regex = "'([A-Z_]+)'".r

  lazy val keywords: Seq[String] = {
    (0 until SqlBaseLexer.VOCABULARY.getMaxTokenType).map { idx =>
      SqlBaseLexer.VOCABULARY.getLiteralName(idx)
    }.collect { case lit if lit != null && regex.pattern.matcher(lit).matches() =>
      regex.findFirstMatchIn(lit).get.group(1)
    }.sorted
  }

  /**
   * Determine whether an input sting is a reserved keyword or not. If a keyword is reserved, then
   * it can not be used as table identifier. The same keyword might be reserved or non-reserved,
   * when ANSI mode is ON/OFF.
   */
  private def isReserved(token: String): Boolean = {
    Try(CatalystSqlParser.parseTableIdentifier(token)).isFailure
  }

  private lazy val nonAnsiReservedList = keywords.map(isReserved)

  private lazy val ansiReservedList = keywords.map(isReserved)

  def getReservedList(): Seq[Boolean] = {
    if (conf.enforceReservedKeywords) ansiReservedList else nonAnsiReservedList
  }
}
