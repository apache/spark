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

import java.util.Map;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.Write;
import org.apache.spark.sql.types.StructType;

/**
 * Represents a table which is staged for being committed to the metastore.
 * <p>
 * This is used to implement atomic CREATE TABLE AS SELECT and REPLACE TABLE AS SELECT queries. The
 * planner will create one of these via
 * {@link StagingTableCatalog#stageCreate(Identifier, StructType, Transform[], Map)} or
 * {@link StagingTableCatalog#stageReplace(Identifier, StructType, Transform[], Map)} to prepare the
 * table for being written to. This table should usually implement {@link SupportsWrite}. A new
 * writer will be constructed via {@link SupportsWrite#newWriteBuilder(LogicalWriteInfo)}, and the
 * write will be committed. The job concludes with a call to {@link #commitStagedChanges()}, at
 * which point implementations are expected to commit the table's metadata into the metastore along
 * with the data that was written by the writes from the write builder this table created.
 *
 * @since 3.0.0
 */
@Evolving
public interface StagedTable extends Table {

  /**
   * Finalize the creation or replacement of this table.
   */
  void commitStagedChanges();

  /**
   * Abort the changes that were staged, both in metadata and from temporary outputs of this
   * table's writers.
   */
  void abortStagedChanges();

  /**
   * Retrieve driver metrics after a commit. This is analogous
   * to {@link Write#reportDriverMetrics()}. Note that these metrics must be included in the
   * supported custom metrics reported by `supportedCustomMetrics` of the
   * {@link StagingTableCatalog} that returned the staged table.
   *
   * @return an Array of commit metric values. Throws if the table has not been committed yet.
   */
  default CustomTaskMetric[] reportDriverMetrics() throws RuntimeException {
      return new CustomTaskMetric[0];
  }
}
