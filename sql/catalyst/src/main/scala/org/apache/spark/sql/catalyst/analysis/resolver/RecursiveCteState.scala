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

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Parameters specific to recursive CTEs.
 *
 * @param cteId The id of the recursive CTE.
 * @param cteName The name of the recursive CTE.
 * @param maxDepth The maximum depth of the recursive CTE.
 */
case class RecursiveCteParameters(cteId: Long, cteName: String, maxDepth: Option[Int])

/**
 * Mutable state for recursive CTEs tracked during resolution.
 *
 * @param anchorOutput The output schema of the anchor branch, used for self-references.
 * @param expectedUnionDepth The depth of the first UNION encountered in the recursive CTE. Used to
 *   ensure anchor registration and UnionLoop placement only occur at the correct depth, preventing
 *   nested UNIONs from interfering.
 * @param columnNames Column names from UnresolvedSubqueryColumnAliases, if present.
 * @param referencedRecursively Whether this CTE has been referenced from within itself.
 */
private[resolver] class RecursiveCteState {
  var anchorOutput: Option[Seq[Attribute]] = None
  var expectedUnionDepth: Option[Int] = None
  var columnNames: Option[Seq[String]] = None
  var referencedRecursively: Boolean = false
}
