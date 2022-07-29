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
import org.apache.spark.sql.connector.expressions.NamedReference;

/**
 * A mix in interface for {@link Scan}. Data sources can implement this interface to
 * report unique keys set to Spark.
 * <p>
 * Spark will optimize the query plan according to the given unique keys.
 * For example, Spark will eliminate the `Distinct` if the v2 relation only output the unique
 * attributes.
 * <pre>
 *   Distinct
 *     +- RelationV2[unique_key#1]
 * </pre>
 * <p>
 * Note that, Spark doest not validate whether the value is unique or not. The implementation
 * should guarantee this.
 *
 * @since 3.4.0
 */
@Evolving
public interface SupportsReportDistinctKeys extends Scan {
    /**
     * Returns a set of unique keys. Each unique keys can consist of multiple attributes.
     */
    Set<Set<NamedReference>> distinctKeysSet();
}
