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

package org.apache.spark.sql.connector.expressions;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.catalog.Table;

/**
 * A reference to a partition field in {@link Table#partitioning()}.
 * <p>
 * {@link #fieldNames()} returns the partition field name (or names) as reported by
 * the table's partition schema.
 * {@link #ordinal()} returns the 0-based position in {@link Table#partitioning()}.
 *
 * @since 4.2.0
 */
@Evolving
public interface PartitionFieldReference extends NamedReference {

  /**
   * Returns the 0-based ordinal of this partition field in {@link Table#partitioning()}.
   */
  int ordinal();
}
