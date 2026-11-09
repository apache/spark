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

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, UserDefinedExpression}
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction

/**
 * The function calls of an expression that a table stores as SQL text, such as the condition of a
 * CHECK constraint or a generation expression. A write reads the text again and resolves the names
 * of the functions in its own session, so these calls must not depend on that session.
 */
object StoredExpressionFunctions {

  /**
   * Returns the first call of a user-defined function in `expr` and the quoted name of the
   * function. In the session of a write, the name can refer to another function or to none.
   */
  def findUserDefinedFunctionCall(expr: Expression): Option[(Expression, String)] = {
    expr.collectFirst {
      case f: SQLFunctionExpression => (f, toSQLId(f.function.name.nameParts))
      case f: UserDefinedExpression => (f, toSQLId(Seq(f.name)))
      case i: StaticInvoke if i.scalarFunction.isDefined =>
        (i, toSQLId(Seq(i.scalarFunction.get.name)))
      case i @ Invoke(Literal(f: ScalarFunction[_], _), _, _, _, _, _, _, _) =>
        (i, toSQLId(Seq(f.name)))
    }
  }

  /**
   * Qualifies the built-in functions that a parsed text calls, so that a temporary function or the
   * SQL path of the session of a write cannot shadow them. Any other name is left as is: a
   * user-defined function in a text that the DDL no longer accepts still resolves here.
   * A function injected with `SparkSessionExtensions.injectFunction` is not in
   * `FunctionRegistry.builtin`, so it is left as is too and can still be shadowed.
   */
  def qualifyBuiltinFunctions(expr: Expression): Expression = expr.transform {
    case f @ UnresolvedFunction(Seq(name), _, _, _, _, _, _, _)
        if FunctionRegistry.builtin.functionExists(FunctionIdentifier(name)) =>
      f.copy(nameParts =
        Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.BUILTIN_NAMESPACE, name))
  }
}
