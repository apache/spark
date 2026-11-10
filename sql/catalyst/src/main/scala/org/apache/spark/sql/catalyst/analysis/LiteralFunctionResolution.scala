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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.{Alias, CurrentDate, CurrentTime, CurrentTimestamp, CurrentUser, Expression, GroupingID, NamedExpression, VirtualColumn}
import org.apache.spark.sql.catalyst.util.toPrettySQL

/**
 * Resolves literal functions by mapping them to their ''real'' function counterparts.
 */
object LiteralFunctionResolution {
  /**
   * Literal functions do not require the user to specify braces when calling them
   * When an attributes is not resolvable, we try to resolve it as a literal function.
   */
  def resolve(nameParts: Seq[String]): Option[NamedExpression] = {
    find(nameParts).map {
      case (_, getFuncExpr, getAliasName) =>
        val funcExpr = getFuncExpr()
        Alias(funcExpr, getAliasName(funcExpr))()
    }
  }

  /** Whether `nameParts` is the name of a literal function, which `resolve` resolves. */
  def isLiteralFunction(nameParts: Seq[String]): Boolean = find(nameParts).isDefined

  private def find(
      nameParts: Seq[String]): Option[(String, () => Expression, Expression => String)] = {
    if (nameParts.length != 1) return None
    literalFunctions.find(func => caseInsensitiveResolution(func._1, nameParts.head))
  }

  // support CURRENT_DATE, CURRENT_TIMESTAMP, CURRENT_TIME,
  //  CURRENT_USER, USER, SESSION_USER and grouping__id
  private val literalFunctions: Seq[(String, () => Expression, Expression => String)] = Seq(
    (CurrentDate().prettyName, () => CurrentDate(), e => toPrettySQL(e)),
    (CurrentTimestamp().prettyName, () => CurrentTimestamp(), e => toPrettySQL(e)),
    (CurrentTime().prettyName, () => CurrentTime(), e => toPrettySQL(e)),
    ("localtime", () => CurrentTime(), e => toPrettySQL(e)),
    (CurrentUser().prettyName, () => CurrentUser(), e => toPrettySQL(e)),
    ("user", () => CurrentUser(), e => toPrettySQL(e)),
    ("session_user", () => CurrentUser(), e => toPrettySQL(e)),
    (VirtualColumn.hiveGroupingIdName, () => GroupingID(Nil), _ => VirtualColumn.hiveGroupingIdName)
  )
}
