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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsRuntimeV2Filtering;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A logical representation of a data source DELETE, UPDATE, or MERGE operation that requires
 * rewriting data.
 *
 * @since 3.3.0
 */
@Experimental
public interface RowLevelOperation {

  /**
   * A row-level SQL command.
   *
   * @since 3.3.0
   */
  enum Command {
    DELETE, UPDATE, MERGE
  }

  /**
   * Returns the description associated with this row-level operation.
   */
  default String description() {
    return this.getClass().toString();
  }

  /**
   * Returns the SQL command that is being performed.
   */
  Command command();

  /**
   * Returns a {@link ScanBuilder} to configure a {@link Scan} for this row-level operation.
   * <p>
   * Data sources fall into two categories: those that can handle a delta of rows and those that
   * need to replace groups (e.g. partitions, files). Data sources that handle deltas allow Spark
   * to quickly discard unchanged rows and have no requirements for input scans. Data sources that
   * replace groups of rows can discard deleted rows but need to keep unchanged rows to be passed
   * back into the source. This means that scans for such data sources must produce all rows
   * in a group if any are returned. Some data sources will avoid pushing filters into files (file
   * granularity), while others will avoid pruning files within a partition (partition granularity).
   * <p>
   * For example, if a data source can only replace partitions, all rows from a partition must
   * be returned by the scan, even if a filter can narrow the set of changes to a single file
   * in the partition. Similarly, a data source that can swap individual files must produce all
   * rows from files where at least one record must be changed, not just rows that must be changed.
   * <p>
   * Data sources that replace groups of data (e.g. files, partitions) may prune entire groups
   * using provided data source filters when building a scan for this row-level operation.
   * However, such data skipping is limited as not all expressions can be converted into data source
   * filters and some can only be evaluated by Spark (e.g. subqueries). Since rewriting groups is
   * expensive, Spark allows group-based data sources to filter groups at runtime. The runtime
   * filtering enables data sources to narrow down the scope of rewriting to only groups that must
   * be rewritten. If the row-level operation scan implements {@link SupportsRuntimeV2Filtering},
   * Spark will execute a query at runtime to find which records match the row-level condition.
   * The runtime group filter subquery will leverage a regular batch scan, which isn't required to
   * produce all rows in a group if any are returned. The information about matching records will
   * be passed back into the row-level operation scan, allowing data sources to discard groups
   * that don't have to be rewritten.
   */
  ScanBuilder newScanBuilder(CaseInsensitiveStringMap options);

  /**
   * Returns a {@link WriteBuilder} to configure a {@link Write} for this row-level operation.
   * <p>
   * Note that Spark will first configure the scan and then the write, allowing data sources to pass
   * information from the scan to the write. For example, the scan can report which condition was
   * used to read the data that may be needed by the write under certain isolation levels.
   * Implementations may capture the built scan or required scan information and then use it
   * while building the write.
   */
  WriteBuilder newWriteBuilder(LogicalWriteInfo info);

  /**
   * Returns metadata attributes that are required to perform this row-level operation.
   * <p>
   * Data sources that can use this method to project metadata columns needed for writing
   * the data back (e.g. metadata columns for grouping data).
   */
  default NamedReference[] requiredMetadataAttributes() {
    return new NamedReference[0];
  }
}
