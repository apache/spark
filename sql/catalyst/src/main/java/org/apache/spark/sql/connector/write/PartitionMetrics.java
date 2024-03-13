/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.connector.write;

import org.apache.spark.util.LongAccumulator;

public class PartitionMetrics {

  private final LongAccumulator numBytes = new LongAccumulator();

  private final LongAccumulator numRecords = new LongAccumulator();

  private final LongAccumulator numFiles = new LongAccumulator();

  public LongAccumulator getNumBytes() {
    return numBytes;
  }

  public LongAccumulator getNumRecords() {
    return numRecords;
  }

  public LongAccumulator getNumFiles() {
    return numFiles;
  }

  public void updateFile(long bytes, long records) {
    this.numBytes.add(bytes);
    this.numRecords.add(records);
    this.numFiles.add(1);
  }

  public void merge (PartitionMetrics otherMetrics) {
    numBytes.merge(otherMetrics.numBytes);
    numRecords.merge(otherMetrics.numRecords);
    numFiles.merge(otherMetrics.numFiles);
  }

  @Override
  public String toString() {
    return "PartitionMetrics{"
        + "numBytes="
        + numBytes.value()
        + ", numRecords="
        + numRecords.value()
        + ", numFiles="
        + numFiles.value()
        + '}';
  }
}
