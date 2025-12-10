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

package org.apache.spark.sql.execution.datasources;

import java.util.Arrays;
import java.util.Objects;

import org.apache.spark.sql.connector.read.VariantExtraction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;

/**
 * Implementation of {@link VariantExtraction}.
 */
public final class VariantExtractionImpl implements VariantExtraction {
  private final String[] columnName;
  private final DataType expectedDataType;
  private final Metadata metadata;

  /**
   * Creates a variant extraction.
   *
   * @param columnName Path to the variant column (e.g., ["v"] for top-level,
   *                   ["struct1", "v"] for nested)
   * @param metadata The JSON path for extraction (e.g., "$.a", "$.b.c")
   * @param expectedDataType The expected data type for the extracted value
   */
  public VariantExtractionImpl(String[] columnName, Metadata metadata, DataType expectedDataType) {
    this.columnName = Objects.requireNonNull(columnName, "columnName cannot be null");
    this.metadata = Objects.requireNonNull(metadata, "metadata cannot be null");
    this.expectedDataType =
        Objects.requireNonNull(expectedDataType, "expectedDataType cannot be null");
    if (columnName.length == 0) {
      throw new IllegalArgumentException("columnName cannot be empty");
    }
  }

  @Override
  public String[] columnName() {
    return columnName;
  }

  @Override
  public DataType expectedDataType() {
    return expectedDataType;
  }

  @Override
  public Metadata metadata() { return metadata; }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    VariantExtractionImpl that = (VariantExtractionImpl) o;
    return Arrays.equals(columnName, that.columnName) &&
            metadata.equals(that.metadata) &&
           expectedDataType.equals(that.expectedDataType);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(metadata, expectedDataType);
    result = 31 * result + Arrays.hashCode(columnName);
    return result;
  }

  @Override
  public String toString() {
    return "VariantExtraction{" +
           "columnName=" + Arrays.toString(columnName) +
           ", metadata='" + metadata + '\'' +
           ", expectedDataType=" + expectedDataType +
           '}';
  }
}
