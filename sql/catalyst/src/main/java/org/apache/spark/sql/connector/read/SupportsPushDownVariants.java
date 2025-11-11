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

import org.apache.spark.annotation.Evolving;

/**
 * A mix-in interface for {@link Scan}. Data sources can implement this interface to
 * support pushing down variant field access operations to the data source.
 * <p>
 * When variant columns are accessed with specific field extractions (e.g., variant_get),
 * the optimizer can push these accesses down to the data source. The data source can then
 * read only the required fields from variant columns, reducing I/O and improving performance.
 * <p>
 * The typical workflow is:
 * <ol>
 *   <li>Optimizer analyzes the query plan and identifies variant field accesses</li>
 *   <li>Optimizer calls {@link #pushVariantAccess} with the access information</li>
 *   <li>Data source validates and stores the variant access information</li>
 *   <li>Optimizer retrieves pushed information via {@link #pushedVariantAccess}</li>
 *   <li>Data source uses the information to optimize reading in {@link #readSchema()}
 *   and readers</li>
 * </ol>
 *
 * @since 4.1.0
 */
@Evolving
public interface SupportsPushDownVariants extends Scan {

  /**
   * Pushes down variant field access information to the data source.
   * <p>
   * Implementations should validate if the variant accesses can be pushed down based on
   * the data source's capabilities. If some accesses cannot be pushed down, the implementation
   * can choose to:
   * <ul>
   *   <li>Push down only the supported accesses and return true</li>
   *   <li>Reject all pushdown and return false</li>
   * </ul>
   * <p>
   * The implementation should store the variant access information that can be pushed down.
   * The stored information will be retrieved later via {@link #pushedVariantAccess()}.
   *
   * @param variantAccessInfo Array of variant access information, one per variant column
   * @return true if at least some variant accesses were pushed down, false if none were pushed
   */
  boolean pushVariantAccess(VariantAccessInfo[] variantAccessInfo);

  /**
   * Returns the variant access information that has been pushed down to this scan.
   * <p>
   * This method is called by the optimizer after {@link #pushVariantAccess} to retrieve
   * what variant accesses were actually accepted by the data source. The optimizer uses
   * this information to rewrite the query plan.
   * <p>
   * If {@link #pushVariantAccess} was not called or returned false, this should return
   * an empty array.
   *
   * @return Array of pushed down variant access information
   */
  VariantAccessInfo[] pushedVariantAccess();
}
