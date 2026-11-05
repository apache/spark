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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.HintInfo

/**
 * Carries the information needed to apply an enclosing query hint to matching
 * [[SubqueryAlias]] nodes during bottom-up resolution.
 *
 * Instances are installed by [[HintResolver]] on the [[OperatorResolutionContext]] that
 * corresponds to the enclosing [[UnresolvedHint]] and discovered by walking up the
 * [[OperatorResolutionContextStack]] when a [[SubqueryAlias]] is resolved.
 *
 * @param relationNames Set of multi-part relation identifiers from the hint parameters that
 *     should be matched against [[SubqueryAlias]] identifiers.
 * @param hintInfoFactory Factory that builds the [[HintInfo]] to attach to a matched alias,
 *     given the alias's output attributes. Avoids forcing the alias to recompute `plan.output`.
 */
class HintApplicationContext(
    val relationNames: Set[Seq[String]],
    val hintInfoFactory: Seq[Attribute] => HintInfo) {

  /**
   * Mutable set that records every relation identifier in `relationNames` that was matched
   * during resolution. Read by the enclosing [[HintResolver]] after the hint's child has been
   * resolved so that unmatched identifiers can be reported through the hint error handler.
   */
  val matchedRelations: mutable.HashSet[Seq[String]] = new mutable.HashSet[Seq[String]]
}
