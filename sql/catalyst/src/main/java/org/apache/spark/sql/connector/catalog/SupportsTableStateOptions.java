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

import java.util.Set;

import org.apache.spark.annotation.Evolving;

/**
 * A catalog capability for identifying options that select a table's state.
 * <p>
 * Spark may resolve the same table more than once while analyzing or refreshing one query. A
 * catalog can implement this interface to declare which raw read options may cause
 * {@link TableCatalog#loadTable(Identifier, TableContext,
 * org.apache.spark.sql.util.CaseInsensitiveStringMap)} to select a different table state, such as
 * a branch, tag, snapshot, or version. Spark can then reuse one concrete {@link Table} instance
 * for references whose table-state options match while preserving every reference's complete
 * option map for scan planning.
 * <p>
 * Option key matching is case-insensitive. Option values remain case-sensitive. Parsed Spark time
 * travel is handled independently and must not be included in the returned set.
 * <p>
 * Catalogs that do not implement this capability are handled conservatively: Spark treats every
 * raw option as table-state-affecting.
 *
 * @since 4.3.0
 */
@Evolving
public interface SupportsTableStateOptions extends CatalogPlugin {

  /**
   * Returns the raw option keys that may affect the table state selected by {@code loadTable}.
   *
   * @return a non-null set of case-insensitive option keys
   */
  Set<String> tableStateOptionKeys();
}
