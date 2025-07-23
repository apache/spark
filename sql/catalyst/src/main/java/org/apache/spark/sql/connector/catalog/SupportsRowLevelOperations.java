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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperation;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A mix-in interface for {@link Table} row-level operations support. Data sources can implement
 * this interface to indicate they support rewriting data for DELETE, UPDATE, MERGE operations.
 *
 * @since 3.3.0
 */
@Experimental
public interface SupportsRowLevelOperations extends Table {
  /**
   * Returns a {@link RowLevelOperationBuilder} to build a {@link RowLevelOperation}.
   * Spark will call this method while planning DELETE, UPDATE and MERGE operations
   * that require rewriting data.
   *
   * @param info the row-level operation info such as command (e.g. DELETE) and options
   * @return the row-level operation builder
   */
  RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info);


  /**
   * Calculate target table schema for MERGE INTO schema evolution.
   * @param sourceTableSchema schema of the source table for MERGE INTO operation.
   * @return new schema for the target table
   */
  default StructType mergeSchema(StructType sourceTableSchema) {
    return CatalogV2Util.v2ColumnsToStructType(columns());
  }

  /**
   * Update schema for MERGE INTO schema evolution.
   * @param newSchema schema to apply to target table for MERGE INTO operation. This
   *                          will be from the return value of #{@link #mergeSchema(StructType)}
   */
  default void updateSchema(StructType newSchema) {
  }
}
