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
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.filter.StorageFilter;
import org.apache.spark.sql.connector.expressions.filter.StorageFilterNotAppliedException;

/**
 * A mix-in interface for {@link Scan}. A data source implements it if its readers can evaluate a
 * {@link StorageFilter}, to skip reading the other columns of the rows the filter rejects. The
 * readers get the filter in the task, through
 * {@link PartitionReaderFactory#createReader(InputPartition, StorageFilterRequest)} or
 * {@link PartitionReaderFactory#createColumnarReader(InputPartition, StorageFilterRequest)}, which
 * the scan's reader factory must implement.
 * <p>
 * Spark offers storage filters only under
 * {@code spark.sql.sources.v2.storageFilterPushdown.enabled}, and only runtime bloom filters for
 * now.
 * <p>
 * This is not partition pruning. A storage filter drops no {@link InputPartition}, and
 * {@link SupportsRuntimeV2Filtering} is the interface for that. A scan may implement both.
 * <p>
 * Applying the filter is optional, per unit the reader chooses, such as a file or a row group.
 * Spark keeps the predicate in the plan above the scan, so a reader may read a unit without it,
 * as a plain read does. A reader does that when it cannot use the filter on the unit, or for the
 * rest of the unit once the filter throws {@link StorageFilterNotAppliedException}.
 * <p>
 * When {@link StorageFilterRequest#appendsCheckedColumn()} is true, the reader also marks the rows
 * it applied the filter to.
 *
 * @since 5.0.0
 */
@Evolving
public interface SupportsStorageFilters extends Scan {

  /**
   * Returns the columns this scan can evaluate a {@link StorageFilter} on. Spark offers the scan a
   * filter only when every column the filter references is one of these. A nested reference
   * counts as its top-level column, which {@link StorageFilter#references()} then names.
   * <p>
   * Declare only columns of {@link Scan#readSchema()}. Spark resolves these against the scan's
   * output when it has a filter to offer, and a column pruned out of it fails the query.
   */
  NamedReference[] storageFilterAttributes();
}
