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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link Table} that declares custom predicate functions
 * available for this table. These functions are registered into the function
 * resolution scope during analysis when queries reference this table.
 * <p>
 * Custom predicate functions appear as regular functions in SQL and DataFrame APIs.
 * During pushdown they are translated to V2 predicates whose name is the descriptor's
 * canonical name (dot-qualified). The data source receives these predicates via
 * {@link org.apache.spark.sql.connector.read.SupportsPushDownV2Filters#pushPredicates}.
 * <p>
 * If a custom predicate is not pushed down (e.g., the data source rejects it), the
 * query will fail with a clear error before execution, since custom predicates cannot
 * be evaluated by Spark.
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsCustomPredicates extends Table {

    /**
     * Returns custom predicate function descriptors for this table.
     * These are resolved during analysis and pushed down during optimization.
     *
     * @return non-null array of custom predicate descriptors; may be empty
     */
    CustomPredicateDescriptor[] customPredicates();
}
