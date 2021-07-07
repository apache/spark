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

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules._

/**
 * Rule that resolves table-valued function references.
 */
case class ResolveTableValuedFunctions(catalog: SessionCatalog) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedTableValuedFunction if u.functionArgs.forall(_.resolved) =>
      withPosition(u) {
        val resolvedFunc = try {
          catalog.lookupTableFunction(u.name, u.functionArgs)
        } catch {
          case _: NoSuchFunctionException =>
            u.failAnalysis(s"could not resolve `${u.name}` to a table-valued function")
        }
        // If alias names assigned, add `Project` with the aliases
        if (u.outputNames.nonEmpty) {
          val outputAttrs = resolvedFunc.output
          // Checks if the number of the aliases is equal to expected one
          if (u.outputNames.size != outputAttrs.size) {
            u.failAnalysis(
              s"Number of given aliases does not match number of output columns. " +
                s"Function name: ${u.name}; number of aliases: " +
                s"${u.outputNames.size}; number of output columns: ${outputAttrs.size}.")
          }
          val aliases = outputAttrs.zip(u.outputNames).map {
            case (attr, name) => Alias(attr, name)()
          }
          Project(aliases, resolvedFunc)
        } else {
          resolvedFunc
        }
      }
  }
}
