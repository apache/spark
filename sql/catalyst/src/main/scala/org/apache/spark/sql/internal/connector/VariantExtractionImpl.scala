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

package org.apache.spark.sql.internal.connector

import org.apache.spark.sql.connector.read.VariantExtraction
import org.apache.spark.sql.types.{DataType, Metadata}

/**
 * Implementation of [[VariantExtraction]].
 *
 * @param columnName Path to the variant column (e.g., Array("v") for top-level,
 *                   Array("struct1", "v") for nested)
 * @param metadata The metadata for extraction including JSON path, failOnError, and timeZoneId
 * @param expectedDataType The expected data type for the extracted value
 */
case class VariantExtractionImpl(
    columnName: Array[String],
    metadata: Metadata,
    expectedDataType: DataType) extends VariantExtraction {

  require(columnName != null, "columnName cannot be null")
  require(metadata != null, "metadata cannot be null")
  require(expectedDataType != null, "expectedDataType cannot be null")
  require(columnName.nonEmpty, "columnName cannot be empty")

  override def toString: String = {
    s"VariantExtraction{columnName=${columnName.mkString("[", ", ", "]")}, " +
      s"metadata='$metadata', expectedDataType=$expectedDataType}"
  }
}
