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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.annotation.InterfaceStability;

import java.util.List;
import java.util.Map;

/**
 * A mix-in interface for {@link DataSourceV2}. Data sources can implement this interface to
 * propagate session configs with chosen key-prefixes to the particular data source.
 */
@InterfaceStability.Evolving
public interface ConfigSupport {

    /**
     * Create a list of key-prefixes, all session configs that match at least one of the prefixes
     * will be propagated to the data source options.
     * If the returned list is empty, no session config will be propagated.
     */
    List<String> getConfigPrefixes();

    /**
     * Create a mapping from session config names to data source option names. If a propagated
     * session config's key doesn't exist in this mapping, the "spark.sql.${source}" prefix will
     * be trimmed. For example, if the data source name is "parquet", perform the following config
     * key mapping by default:
     * "spark.sql.parquet.int96AsTimestamp" -> "int96AsTimestamp",
     * "spark.sql.parquet.compression.codec" -> "compression.codec",
     * "spark.sql.columnNameOfCorruptRecord" -> "columnNameOfCorruptRecord".
     *
     * If the mapping is specified, for example, the returned map contains an entry
     * ("spark.sql.columnNameOfCorruptRecord" -> "colNameCorrupt"), then the session config
     * "spark.sql.columnNameOfCorruptRecord" will be converted to "colNameCorrupt" in
     * [[DataSourceV2Options]].
     */
    Map<String, String> getConfigMapping();

    /**
     * Create a list of valid data source option names. When the list is specified, a session
     * config will NOT be propagated if its corresponding option name is not in the list.
     *
     * If the returned list is empty, don't check the option names.
     */
    List<String> getValidOptions();
}
