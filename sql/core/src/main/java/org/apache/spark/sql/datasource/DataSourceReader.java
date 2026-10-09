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

package org.apache.spark.sql.datasource;

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * Reads the data of a {@link DataSource} for a batch scan.
 * <p>
 * Spark creates the reader on the driver with {@link DataSource#reader}. It then pushes
 * operations down by calling, each at most once and in this order, {@link #pushPredicates},
 * {@link #pushLimit} and {@link #pruneColumns}, and plans the scan with {@link #partitions()}.
 * After that, the reader is serialized and sent to the executors, where {@link #read} is called
 * once for each partition. Any state that {@link #read} needs must be set by the time
 * {@link #partitions()} returns.
 *
 * @since 4.4.0
 */
@Evolving
public interface DataSourceReader extends Serializable {

  /**
   * Pushes predicates down to the data source. The predicates are combined with AND.
   *
   * @return the predicates that Spark still has to evaluate after reading. By default, all of
   *         them. The data source can still use a returned predicate to skip data.
   */
  default Predicate[] pushPredicates(Predicate[] predicates) {
    return predicates;
  }

  /**
   * Pushes a LIMIT down to the data source.
   *
   * @return whether the reader returns at most {@code limit} rows in total. Spark applies the
   *         limit again after reading either way.
   */
  default boolean pushLimit(int limit) {
    return false;
  }

  /**
   * Prunes the columns to read.
   *
   * @param requiredSchema the top-level fields of the read schema that the query needs, with
   *                       their full types
   * @return whether {@link #read} returns exactly the columns of {@code requiredSchema}, in that
   *         order. If false, {@link #read} returns all the columns of the read schema.
   */
  default boolean pruneColumns(StructType requiredSchema) {
    return false;
  }

  /**
   * Plans the partitions of the scan. Spark reads each partition in a separate task.
   */
  InputPartition[] partitions();

  /**
   * Reads a partition. Called on an executor.
   * <p>
   * The columns of the returned batches must match the read schema: the schema passed to
   * {@link DataSource#reader}, or the required schema if {@link #pruneColumns} returned true. A
   * batch only needs to stay valid until the next call to {@link PartitionReader#next()}.
   */
  PartitionReader<ColumnarBatch> read(InputPartition partition);
}
