/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * An interface for exposing data columns for a table that are not in the table schema. For example,
 * a file source could expose a "file" column that contains the path of the file that contained each
 * row.
 * <p>
 * The columns returned by {@link #metadataColumns()} may be passed as {@link StructField} in
 * requested projections. Sources that implement this interface and column projection using
 * {@link SupportsPushDownRequiredColumns} must accept metadata fields passed to
 * {@link SupportsPushDownRequiredColumns#pruneColumns(StructType)}.
 * <p>
 * If a table column and a metadata column have the same name, the conflict is resolved by either
 * renaming or suppressing the metadata column. See {@link canRenameConflictingMetadataColumns}.
 *
 * @since 3.1.0
 */
@Evolving
public interface SupportsMetadataColumns extends Table {
  /**
   * Metadata columns that are supported by this {@link Table}.
   * <p>
   * The columns returned by this method may be passed as {@link StructField} in requested
   * projections using {@link SupportsPushDownRequiredColumns#pruneColumns(StructType)}.
   * <p>
   * If a table column and a metadata column have the same name, the conflict is resolved by either
   * renaming or suppressing the metadata column. See {@link canRenameConflictingMetadataColumns}.
   *
   * @return an array of {@link MetadataColumn}
   */
  MetadataColumn[] metadataColumns();

  /**
   * Determines how this data source handles name conflicts between metadata and data columns.
   * <p>
   * If true, spark will automatically rename the metadata column to resolve the conflict. End users
   * can reliably select metadata columns (renamed or not) with {@code Dataset.metadataColumn}, and
   * internal code can use {@code MetadataAttributeWithLogicalName} to extract the logical name from
   * a metadata attribute.
   * <p>
   * If false, the data column will hide the metadata column. It is recommended that Table
   * implementations which do not support renaming should reject data column names that conflict
   * with metadata column names.
   */
  default boolean canRenameConflictingMetadataColumns() { return false; }
}
