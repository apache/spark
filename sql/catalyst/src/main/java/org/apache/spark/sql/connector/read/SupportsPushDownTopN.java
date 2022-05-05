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

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.SortOrder;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * push down top N(query with ORDER BY ... LIMIT n). Please note that the combination of top N
 * with other operations such as AGGREGATE, GROUP BY, CLUSTER BY, DISTRIBUTE BY, etc.
 * is NOT pushed down.
 *
 * @since 3.3.0
 */
@Evolving
public interface SupportsPushDownTopN extends ScanBuilder {

    /**
     * Pushes down top N to the data source.
     */
    boolean pushTopN(SortOrder[] orders, int limit);

    /**
     * Whether the top N is partially pushed or not. If it returns true, then Spark will do top N
     * again. This method will only be called when {@link #pushTopN} returns true.
     */
    default boolean isPartiallyPushed() { return true; }
}
