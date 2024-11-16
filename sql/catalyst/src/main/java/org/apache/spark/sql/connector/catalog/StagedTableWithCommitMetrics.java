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
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.Write;

/**
 * An extension of the {@link StagedTable} interface that allows to retrieve metrics after a commit.
 */
@Evolving
public interface StagedTableWithCommitMetrics extends StagedTable {

    /**
     * Retrieve driver metrics after a commit. This is analogous
     * to {@link Write#reportDriverMetrics()}. Note that these metrics must be included in the
     * supported custom metrics reported by `supportedCustomMetrics` of the
     * {@link StagingTableCatalog} that returned the staged table.
     *
     * @return an Array of commit metric values. Throws if the table has not been committed yet.
     */
    CustomTaskMetric[] reportDriverMetrics() throws AssertionError;
}
