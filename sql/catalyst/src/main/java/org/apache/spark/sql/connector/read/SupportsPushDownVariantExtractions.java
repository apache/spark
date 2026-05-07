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

import org.apache.spark.annotation.Experimental;

/**
 * A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to
 * support pushing down variant field extraction operations to the data source.
 * <p>
 * When variant columns are accessed with specific field extractions (e.g., variant_get,
 * try_variant_get), the optimizer can push these extractions down to the data source.
 * The data source can then read only the required fields from variant columns, reducing
 * I/O and improving performance.
 * <p>
 * Each {@link VariantExtraction} in the input array represents one field extraction operation.
 * Data sources should examine each extraction and determine which ones can be handled efficiently.
 * The return value is a boolean array of the same length, where each element indicates whether
 * the corresponding extraction was accepted.
 *
 * @since 4.1.0
 */
@Experimental
public interface SupportsPushDownVariantExtractions extends ScanBuilder {

  /**
   * Pushes down variant field extractions to the data source.
   * <p>
   * Each element in the input array represents one field extraction operation from a variant
   * column. Data sources should examine each extraction and determine whether it can be
   * pushed down based on the data source's capabilities (e.g., supported data types,
   * path complexity, etc.).
   * <p>
   * The return value is a boolean array of the same length as the input array, where each
   * element indicates whether the corresponding extraction was accepted:
   * <ul>
   *   <li>true: The extraction will be handled by the data source</li>
   *   <li>false: The extraction will be handled by Spark after reading</li>
   * </ul>
   * <p>
   * Data sources can choose to accept all, some, or none of the extractions. Spark will
   * handle any extractions that are not pushed down.
   *
   * @param extractions Array of variant extractions, one per field extraction operation
   * @return Boolean array indicating which extractions were accepted (same length as input)
   */
  boolean[] pushVariantExtractions(VariantExtraction[] extractions);
}
