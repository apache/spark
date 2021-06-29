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

package org.apache.spark.sql.execution.datasources.parquet;

/**
 * Helper class to store intermediate state while reading a Parquet column chunk.
 */
final class ParquetReadState {
  /** Maximum definition level */
  final int maxDefinitionLevel;

  /** The offset in the current batch to put the next value */
  int offset;

  /** The remaining number of values to read in the current page */
  int valuesToReadInPage;

  /** The remaining number of values to read in the current batch */
  int valuesToReadInBatch;

  ParquetReadState(int maxDefinitionLevel) {
    this.maxDefinitionLevel = maxDefinitionLevel;
  }

  /**
   * Called at the beginning of reading a new batch.
   */
  void resetForBatch(int batchSize) {
    this.offset = 0;
    this.valuesToReadInBatch = batchSize;
  }

  /**
   * Called at the beginning of reading a new page.
   */
  void resetForPage(int totalValuesInPage) {
    this.valuesToReadInPage = totalValuesInPage;
  }

  /**
   * Advance the current offset to the new values.
   */
  void advanceOffset(int newOffset) {
    valuesToReadInBatch -= (newOffset - offset);
    valuesToReadInPage -= (newOffset - offset);
    offset = newOffset;
  }
}
