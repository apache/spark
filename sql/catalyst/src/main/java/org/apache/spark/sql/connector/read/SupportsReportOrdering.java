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
import org.apache.spark.sql.connector.expressions.SortOrder;

/**
 * A mix in interface for {@link Scan}. Data sources can implement this interface to
 * report the order of data in each partition to Spark.
 * Global order is part of the partitioning, see {@link SupportsReportPartitioning}.
 * <p>
 * Spark uses ordering information to exploit existing order to avoid sorting required by
 * subsequent operations.
 *
 * @since 3.4.0
 */
@Evolving
public interface SupportsReportOrdering extends Scan {

  /**
   * Returns the order in each partition of this data source scan.
   * <p>
   * Spark resolves the column references in these sort orders against the table columns,
   * including columns pruned from the scan output. If any of them cannot be resolved, Spark
   * ignores the whole reported ordering and logs a warning.
   * <p>
   * Spark currently does not rely on a sort order over a transform such as {@code days(ts)} to
   * avoid a sort, whether or not it is a partition key. When the transform is a partition key and
   * Spark uses the reported {@code KeyGroupedPartitioning}, the sort order is constant within each
   * partition, so Spark skips it and may rely on the sort orders after it. After any other
   * transform, Spark relies only on a sort order on a partition key that is not a transform, when
   * it uses the reported {@code KeyGroupedPartitioning}.
   * <p>
   * If Spark relies on none of the reported sort orders, for example when this returns an empty
   * array, it may treat each partition as sorted by the partition keys that are not transforms.
   * This needs a reported {@code KeyGroupedPartitioning}, whose partitions each hold a single key
   * value. {@code spark.sql.sources.v2.bucketing.partitionKeyOrdering.enabled} controls it.
   */
  SortOrder[] outputOrdering();
}
