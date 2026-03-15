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

package org.apache.spark.sql.connector.read;

import java.util.Set;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link ScanBuilder} that allows data sources to declare
 * extended predicate capabilities beyond the default set. When a ScanBuilder
 * implements this interface, the V2 expression translator will attempt to translate
 * additional builtin Catalyst expressions that match the declared capabilities.
 * <p>
 * This interface works in conjunction with {@link SupportsPushDownV2Filters}.
 * The capabilities declared here enable Spark to <em>translate</em> additional
 * expressions to V2 predicates. The data source may still reject individual
 * predicates at {@link SupportsPushDownV2Filters#pushPredicates} time.
 * <p>
 * Supported predicate names (upper-cased):
 * <ul>
 *   <li>{@code "LIKE"} — full LIKE pattern matching</li>
 *   <li>{@code "RLIKE"} — regex matching</li>
 *   <li>{@code "ILIKE"} — case-insensitive LIKE (non-literal patterns only)</li>
 *   <li>{@code "IS_NAN"} — NaN check for numeric types</li>
 *   <li>{@code "ARRAY_CONTAINS"} — element membership in arrays</li>
 *   <li>{@code "MAP_CONTAINS_KEY"} — key existence in maps</li>
 *   <li>{@code "ARRAYS_OVERLAP"} — array intersection check</li>
 *   <li>{@code "LIKE_ALL"} — match all patterns</li>
 *   <li>{@code "LIKE_ANY"} — match any pattern</li>
 *   <li>{@code "NOT_LIKE_ALL"} — match no patterns (negation of LIKE_ALL)</li>
 *   <li>{@code "NOT_LIKE_ANY"} — not match any (negation of LIKE_ANY)</li>
 * </ul>
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsPushDownPredicateCapabilities extends ScanBuilder {

    /**
     * Returns the set of additional predicate/expression names this data source
     * supports for pushdown, beyond the default always-translated set.
     * <p>
     * Names should be upper-cased. The data source may still reject individual
     * predicates at {@code pushPredicates()} time (e.g., if a particular RLIKE
     * pattern is too complex). This method merely enables Spark to attempt
     * translation.
     *
     * @return non-null set of upper-cased predicate/expression names; may be empty
     */
    Set<String> supportedPredicateNames();
}
