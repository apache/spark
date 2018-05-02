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

import org.apache.spark.annotation.InterfaceStability;

/**
 * Exception thrown when the parquet reader find column type mismatches.
 */
@InterfaceStability.Unstable
public class SchemaColumnConvertNotSupportedException extends RuntimeException {

  /**
   * Name of the column which cannot be converted.
   */
  private String column;
  /**
   * Physical column type in the actual parquet file.
   */
  private String physicalType;
  /**
   * Logical column type in the parquet schema the parquet reader use to parse all files.
   */
  private String logicalType;

  public String getColumn() {
    return column;
  }

  public String getPhysicalType() {
    return physicalType;
  }

  public String getLogicalType() {
    return logicalType;
  }

  public SchemaColumnConvertNotSupportedException(
      String column,
      String physicalType,
      String logicalType) {
    super();
    this.column = column;
    this.physicalType = physicalType;
    this.logicalType = logicalType;
  }
}
