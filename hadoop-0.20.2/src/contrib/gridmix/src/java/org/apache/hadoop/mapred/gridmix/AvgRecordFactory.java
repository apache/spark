/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred.gridmix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * Given byte and record targets, emit roughly equal-sized records satisfying
 * the contract.
 */
class AvgRecordFactory extends RecordFactory {

  /**
   * Percentage of record for key data.
   */
  public static final String GRIDMIX_KEY_FRC = "gridmix.key.fraction";


  private final long targetBytes;
  private final long targetRecords;
  private final long step;
  private final int avgrec;
  private final int keyLen;
  private long accBytes = 0L;
  private long accRecords = 0L;

  /**
   * @param targetBytes Expected byte count.
   * @param targetRecords Expected record count.
   * @param conf Used to resolve edge cases @see #GRIDMIX_KEY_FRC
   */
  public AvgRecordFactory(long targetBytes, long targetRecords,
      Configuration conf) {
    this.targetBytes = targetBytes;
    this.targetRecords = targetRecords <= 0 && this.targetBytes >= 0
      ? Math.max(1,
          this.targetBytes / conf.getInt("gridmix.missing.rec.size", 64 * 1024))
      : targetRecords;
    final long tmp = this.targetBytes / this.targetRecords;
    step = this.targetBytes - this.targetRecords * tmp;
    avgrec = (int) Math.min(Integer.MAX_VALUE, tmp + 1);
    keyLen = Math.max(1,
        (int)(tmp * Math.min(1.0f, conf.getFloat(GRIDMIX_KEY_FRC, 0.1f))));
  }

  @Override
  public boolean next(GridmixKey key, GridmixRecord val) throws IOException {
    if (accBytes >= targetBytes) {
      return false;
    }
    final int reclen = accRecords++ >= step ? avgrec - 1 : avgrec;
    final int len = (int) Math.min(targetBytes - accBytes, reclen);
    // len != reclen?
    if (key != null) {
      key.setSize(keyLen);
      val.setSize(len - key.getSize());
    } else {
      val.setSize(len);
    }
    accBytes += len;
    return true;
  }

  @Override
  public float getProgress() throws IOException {
    return Math.min(1.0f, accBytes / ((float)targetBytes));
  }

  @Override
  public void close() throws IOException {
    // noop
  }

}
