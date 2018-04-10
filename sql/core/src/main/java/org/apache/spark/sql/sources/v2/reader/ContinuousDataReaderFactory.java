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

import org.apache.spark.annotation.InterfaceStability;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.v2.reader.streaming.ContinuousDataReader;
import org.apache.spark.sql.vectorized.ColumnarBatch;

/**
 * A mix-in interface for {@link DataReaderFactory}. Continuous data reader factories can
 * implement this interface to provide creating {@link DataReader} with particular offset.
 */
@InterfaceStability.Evolving
public interface ContinuousDataReaderFactory extends DataReaderFactory {

  /**
   * Returns a row-formatted continuous data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  default ContinuousDataReader<Row> createRowDataReader() {
    throw new IllegalStateException(
      "createRowDataReader must be implemented if the data format is ROW.");
  }

  /**
   * Returns a unsafe-row-formatted continuous data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  default ContinuousDataReader<UnsafeRow> createUnsafeRowDataReader() {
    throw new IllegalStateException(
      "createUnsafeRowDataReader must be implemented if the data format is UNSAFE_ROW.");
  }

  /**
   * Returns a columnar-batch-formatted continuous data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  default ContinuousDataReader<ColumnarBatch> createColumnarBatchDataReader() {
    throw new IllegalStateException(
      "createColumnarBatchDataReader must be implemented if the data format is COLUMNAR_BATCH.");
  }
}
