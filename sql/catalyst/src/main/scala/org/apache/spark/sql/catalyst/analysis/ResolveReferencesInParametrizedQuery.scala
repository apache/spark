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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryCompilationErrors.unresolvedVariableError

/**
 * This class resolves variables in a parametrized query.
 *
 * @param catalogManager Catalog manager
 */
class ResolveReferencesInParametrizedQuery(val catalogManager: CatalogManager)
  extends SQLConfHelper with ColumnResolutionHelper {

  def resolveVariable(e: Expression) : Expression = {
    /* We know that the expression is either UnresolvedAttribute or Alias,
     * as passed from the parser.
     * If it is an UnresolvedAttribute, we look it up in the catalog and return it.
     * If it is an Alias, we resolve the child and return an Alias with the same name.
     */
    e match {
      case u: UnresolvedAttribute =>
        lookupVariable(u.nameParts) match {
          case Some(variable) =>
            variable.copy(canFold = false)
          case _ => throw unresolvedVariableError(u.nameParts, Seq("SYSTEM", "SESSION"))
        }

      case a: Alias =>
        Alias(resolveVariable(a.child), a.name)()

      case other => throw SparkException.internalError(
        "Unexpected variable expression in ParametrizedQuery: " + other)
    }
  }

  def apply(pq: ParameterizedQuery): ParameterizedQuery = {
    def resolveVariables(expressions : Seq[Expression]): Seq[Expression] = {
        expressions.map { exp =>
          if (exp.resolved) {
            exp
          } else {
            resolveVariable(exp)
          }
        }
    }

    pq match {
      case PosParameterizedQuery(child, args) =>
        PosParameterizedQuery(child, resolveVariables(args))
      case NameParameterizedQuery(child, argNames, argValues) =>
        NameParameterizedQuery(child, argNames, resolveVariables(argValues))
    }
  }
}
