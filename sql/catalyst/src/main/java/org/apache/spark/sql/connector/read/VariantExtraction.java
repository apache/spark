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

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;

/**
 * Variant extraction information that describes a single field extraction from a variant column.
 * <p>
 * This interface captures the information needed by data sources to optimize reading variant
 * columns. Each instance represents one field extraction operation (e.g., from variant_get or
 * try_variant_get).
 * <p>
 * For example, if a query contains `variant_get(v, '$.a', 'int')`, this would be represented
 * as a VariantExtraction with columnName=["v"], path="$.a", and expectedDataType=IntegerType.
 *
 * @since 4.1.0
 */
@Experimental
public interface VariantExtraction extends Serializable {
  /**
   * Returns the path to the variant column. For top-level variant columns, this is a single
   * element array containing the column name. For nested variant columns within structs,
   * this is an array representing the path (e.g., ["structCol", "innerStruct", "variantCol"]).
   */
  String[] columnName();

  /**
   * Returns the expected data type for the extracted value.
   * This is the target type specified in variant_get (e.g., IntegerType, StringType).
   */
  DataType expectedDataType();

  /**
   * Returns the metadata associated with this variant extraction.
   * This may include additional information needed by the data source:
   * - "path": the extraction path from variant_get or try_variant_get.
   *           This follows JSON path syntax (e.g., "$.a", "$.b.c", "$[0]").
   * - "failOnError": whether the extraction to expected data type should throw an exception
   *                  or return null if the cast fails.
   * - "timeZoneId": a string identifier of a time zone. It is required by timestamp-related casts.
   *
   */
  Metadata metadata();
}
