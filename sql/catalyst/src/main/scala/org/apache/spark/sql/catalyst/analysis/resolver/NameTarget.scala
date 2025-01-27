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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.util.StringUtils.orderSuggestedIdentifiersBySimilarity
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Class that represents results of name resolution or star expansion. It encapsulates:
 *   - `candidates` - A list of candidates that are possible matches for a given name.
 *   - `aliasName` - If the candidates size is 1 and it's type is `ExtractValue` (which means that
 *     it's a recursive type), then the `aliasName` should be the name with which the candidate is
 *     aliased. Otherwise, `aliasName` should be `None`.
 *   - `allAttributes` - A list of all attributes which is used to generate suggestions for
 *     unresolved column error.
 *
 * Example:
 *
 * - Attribute resolution:
 * {{{ SELECT col1 FROM VALUES (1); }}} will have a [[NameTarget]] with a single candidate `col1`.
 * `aliasName` would be `None` in this case because the column is not of recursive type.
 *
 * - Recursive attribute resolution:
 * {{{ SELECT col1.col1 FROM VALUES(STRUCT(1,2), 3) }}} will have a [[NameTarget]] with a
 * single candidate `col1` and an `aliasName` of `Some("col1")`.
 */
case class NameTarget(
    candidates: Seq[Expression],
    aliasName: Option[String] = None,
    allAttributes: Seq[Attribute] = Seq.empty) {

  /**
   * Picks a candidate from the list of candidates based on the given unresolved attribute.
   * Its behavior is as follows (based on the number of candidates):
   *
   * - If there is only one candidate, it will be returned.
   *
   * - If there are multiple candidates, an ambiguous reference error will be thrown.
   *
   * - If there are no candidates, an unresolved column error will be thrown.
   */
  def pickCandidate(unresolvedAttribute: UnresolvedAttribute): Expression = {
    candidates match {
      case Seq() =>
        throwUnresolvedColumnError(unresolvedAttribute)
      case Seq(candidate) =>
        candidate
      case _ =>
        throw QueryCompilationErrors.ambiguousReferenceError(
          unresolvedAttribute.name,
          candidates.collect { case attribute: AttributeReference => attribute }
        )
    }
  }

  private def throwUnresolvedColumnError(unresolvedAttribute: UnresolvedAttribute): Nothing =
    throw QueryCompilationErrors.unresolvedColumnError(
      unresolvedAttribute.name,
      proposal = orderSuggestedIdentifiersBySimilarity(
        unresolvedAttribute.name,
        candidates = allAttributes.map(attribute => attribute.qualifier :+ attribute.name)
      )
    )
}
