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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * The [[DelegatesResolutionToExtensions]] is a trait which provides a method to delegate the
 * resolution of unresolved operators to a list of [[ResolverExtension]]s.
 */
trait DelegatesResolutionToExtensions {

  protected val extensions: Seq[ResolverExtension]

  /**
   * Find the suitable extension for `unresolvedOperator` resolution and resolve it with that
   * extension. Usually extensions return resolved relation nodes, so we generically update the name
   * scope without matching for specific relations, for simplicity.
   *
   * We match the extension once to reduce the number of
   * [[ResolverExtension.resolveOperator.isDefinedAt]] calls, because those can be expensive.
   *
   * @returns `Some(resolutionResult)` if the extension was found and `unresolvedOperator` was
   * resolved, `None` otherwise.
   *
   * @throws `AMBIGUOUS_RESOLVER_EXTENSION` if there were several matched extensions for this
   * operator.
   */
  def tryDelegateResolutionToExtension(unresolvedOperator: LogicalPlan): Option[LogicalPlan] = {
    var resolutionResult: Option[LogicalPlan] = None
    var matchedExtension: Option[ResolverExtension] = None
    extensions.foreach { extension =>
      matchedExtension match {
        case None =>
          resolutionResult = extension.resolveOperator.lift(unresolvedOperator)

          if (resolutionResult.isDefined) {
            matchedExtension = Some(extension)
          }
        case Some(matchedExtension) =>
          if (extension.resolveOperator.isDefinedAt(unresolvedOperator)) {
            throw QueryCompilationErrors
              .ambiguousResolverExtension(
                unresolvedOperator,
                Seq(matchedExtension, extension).map(_.getClass.getSimpleName)
              )
              .withPosition(unresolvedOperator.origin)
          }
      }
    }

    resolutionResult
  }
}
