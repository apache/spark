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

package org.apache.spark.sql.sources.v2.reader;

import java.io.Serializable;

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A factory of {@link PartitionReader}s. Implementations can do either row-based scan or columnar
 * scan, by switching the {@link #supportColumnarReads()} flag.
 */
@InterfaceStability.Evolving
public interface PartitionReaderFactory extends Serializable {

  /**
   * Returns a row-based partition reader to do the actual data reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  PartitionReader<InternalRow> createReader(InputPartition partition);

  /**
   * Returns a columnar partition reader to do the actual data reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  default PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new UnsupportedOperationException("Cannot create columnar reader.");
  }

  /**
   * If this method returns true, Spark will call {@link #createColumnarReader(InputPartition)} to
   * create the {@link PartitionReader} and scan the data in a columnar way. This means,
   * implementations must also implement {@link #createColumnarReader(InputPartition)} when true
   * is returned here.
   */
  default boolean supportColumnarReads() {
    return false;
  }
}
