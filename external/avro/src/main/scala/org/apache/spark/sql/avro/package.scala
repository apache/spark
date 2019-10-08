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

package org.apache.spark.sql

package object avro {

  /**
   * Converts a binary column of avro format into its corresponding catalyst value. The specified
   * schema must match the read data, otherwise the behavior is undefined: it may fail or return
   * arbitrary result.
   *
   * @param data the binary column.
   * @param jsonFormatSchema the avro schema in JSON string format.
   *
   * @since 2.4.0
   */
  @deprecated("Please use 'org.apache.spark.sql.avro.functions.from_avro' instead.", "3.0.0")
  def from_avro(
      data: Column,
      jsonFormatSchema: String): Column =
    org.apache.spark.sql.avro.functions.from_avro(data, jsonFormatSchema)

  /**
   * Converts a column into binary of avro format.
   *
   * @param data the data column.
   *
   * @since 2.4.0
   */
  @deprecated("Please use 'org.apache.spark.sql.avro.functions.to_avro' instead.", "3.0.0")
  def to_avro(data: Column): Column = org.apache.spark.sql.avro.functions.to_avro(data)
}
