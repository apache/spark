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

package org.apache.spark.sql.connector.read.streaming;

import org.apache.spark.SparkUnsupportedOperationException;
import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A variation on {@link PartitionReaderFactory} that returns {@link ContinuousPartitionReader}
 * instead of {@link PartitionReader}. It's used for continuous streaming processing.
 *
 * @since 3.0.0
 */
@Evolving
public interface ContinuousPartitionReaderFactory extends PartitionReaderFactory {
  @Override
  ContinuousPartitionReader<InternalRow> createReader(InputPartition partition);

  @Override
  default ContinuousPartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
    throw new SparkUnsupportedOperationException("_LEGACY_ERROR_TEMP_3150");
  }
}
