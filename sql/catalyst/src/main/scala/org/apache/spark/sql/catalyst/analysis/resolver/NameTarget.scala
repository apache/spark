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
import org.apache.spark.sql.types.Metadata

/**
 * [[NameTarget]] is a result of a multipart name resolution of the
 * [[NameScope.resolveMultipartName]].
 *
 * Attribute resolution:
 *
 * {{{
 * -- [[NameTarget]] with a single candidate `col1`. `aliasName` is be `None` in this case because
 * -- the name is not a field/value/item of some recursive type.
 * SELECT col1 FROM VALUES (1);
 * }}}
 *
 * Attribute resolution ambiguity:
 *
 * {{{
 * -- [[NameTarget]] with candidates `col1`, `col1`. [[pickCandidate]] will throw
 * -- `AMBIGUOUS_REFERENCE`.
 * SELECT col1 FROM VALUES (1) t1, VALUES (2) t2;
 * }}}
 *
 * Struct field resolution:
 *
 * {{{
 * -- [[NameTarget]] with a single candidate `GetStructField(col1, "field1")`. `aliasName` is
 * -- `Some("col1")`, since here we extract a field of a struct.
 * SELECT col1.field1 FROM VALUES (named_struct('field1', 1), 3);
 * }}}
 *
 * @param candidates A list of candidates that are possible matches for a given name.
 * @param aliasName If the candidates size is 1 and it's type is [[ExtractValue]] (which means that
 *   it's a field/value/item from a recursive type), then the `aliasName` should be the name with
 *   which the candidate needs to be aliased. Otherwise, `aliasName` is `None`.
 * @param aliasMetadata If the candidates were created out of expressions referenced by group by
 *   alias, store the metadata of the alias. Otherwise, `aliasMetadata` is `None`.
 * @param lateralAttributeReference If the candidate is laterally referencing another column this
 *   field is populated with that column's attribute.
 * @param output [[output]] of a [[NameScope]] that produced this [[NameTarget]]. Used to provide
 *   suggestions for thrown errors.
 * @param isOuterReference A flag indicating that this [[NameTarget]] resolves to an outer
 *   reference.
 */
case class NameTarget(
    candidates: Seq[Expression],
    aliasName: Option[String] = None,
    aliasMetadata: Option[Metadata] = None,
    lateralAttributeReference: Option[Attribute] = None,
    output: Seq[Attribute] = Seq.empty,
    isOuterReference: Boolean = false) {

  /**
   * Pick a single candidate from `candidates`:
   * - If there are no candidates, throw `UNRESOLVED_COLUMN.WITH_SUGGESTION`.
   * - If there are several candidates, throw `AMBIGUOUS_REFERENCE`.
   * - Otherwise, return a single [[Expression]].
   */
  def pickCandidate(unresolvedAttribute: UnresolvedAttribute): Expression = {
    if (candidates.isEmpty) {
      throwUnresolvedColumnError(unresolvedAttribute)
    }
    if (candidates.length > 1) {
      throwAmbiguousReferenceError(unresolvedAttribute)
    }
    candidates.head
  }

  private def throwUnresolvedColumnError(unresolvedAttribute: UnresolvedAttribute): Nothing =
    throw QueryCompilationErrors.unresolvedColumnError(
      unresolvedAttribute.name,
      proposal = orderSuggestedIdentifiersBySimilarity(
        unresolvedAttribute.name,
        candidates = output.map(attribute => attribute.qualifier :+ attribute.name)
      )
    )

  private def throwAmbiguousReferenceError(unresolvedAttribute: UnresolvedAttribute): Nothing =
    throw QueryCompilationErrors.ambiguousReferenceError(
      unresolvedAttribute.name,
      candidates.collect { case attribute: AttributeReference => attribute }
    )
}
