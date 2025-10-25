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

import java.io.Serializable;
import java.util.Objects;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * Variant access information that describes how variant fields are accessed in a query.
 * <p>
 * This class captures the information needed by data sources to optimize reading variant columns.
 * Instead of reading the entire variant value, the data source can read only the fields that
 * are actually accessed, represented as a structured schema.
 * <p>
 * For example, if a query accesses `variant_get(v, '$.a', 'int')` and
 * `variant_get(v, '$.b', 'string')`, the extracted schema would be
 * `struct&lt;0:int, 1:string&gt;` where field ordinals correspond to the access order.
 *
 * @since 4.1.0
 */
@Evolving
public final class VariantAccessInfo implements Serializable {
  private final String columnName;
  private final StructType extractedSchema;

  /**
   * Creates variant access information for a variant column.
   *
   * @param columnName The name of the variant column
   * @param extractedSchema The schema representing extracted fields from the variant.
   *                       Each field represents one variant field access, with field names
   *                       typically being ordinals (e.g., "0", "1", "2") and metadata
   *                       containing variant-specific information like JSON path.
   */
  public VariantAccessInfo(String columnName, StructType extractedSchema) {
    this.columnName = Objects.requireNonNull(columnName, "columnName cannot be null");
    this.extractedSchema =
            Objects.requireNonNull(extractedSchema, "extractedSchema cannot be null");
  }

  /**
   * Returns the name of the variant column.
   */
  public String columnName() {
    return columnName;
  }

  /**
   * Returns the schema representing fields extracted from the variant column.
   * <p>
   * The schema structure is:
   * <ul>
   *   <li>Field names: Typically ordinals ("0", "1", "2", ...) representing access order</li>
   *   <li>Field types: The target data type for each field extraction</li>
   *   <li>Field metadata: Contains variant-specific information such as JSON path,
   *       timezone, and error handling mode</li>
   * </ul>
   * <p>
   * Data sources should use this schema to determine what fields to extract from the variant
   * and what types they should be converted to.
   */
  public StructType extractedSchema() {
    return extractedSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariantAccessInfo that = (VariantAccessInfo) o;
    return columnName.equals(that.columnName) &&
           extractedSchema.equals(that.extractedSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName, extractedSchema);
  }

  @Override
  public String toString() {
    return "VariantAccessInfo{" +
           "columnName='" + columnName + '\'' +
           ", extractedSchema=" + extractedSchema +
           '}';
  }
}
