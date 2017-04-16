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
package org.apache.hadoop.util;

import java.io.IOException;

public class ChecksumUtil {
  /**
   * updates the checksum for a buffer
   * 
   * @param buf - buffer to update the checksum in
   * @param chunkOff - offset in the buffer where the checksum is to update
   * @param dataOff - offset in the buffer of the data
   * @param dataLen - length of data to compute checksum on
   */
  public static void updateChunkChecksum(
    byte[] buf,
    int checksumOff,
    int dataOff, 
    int dataLen,
    DataChecksum checksum
  ) throws IOException {
    int bytesPerChecksum = checksum.getBytesPerChecksum();
    int checksumSize = checksum.getChecksumSize();
    int curChecksumOff = checksumOff;
    int curDataOff = dataOff;
    int numChunks = (dataLen + bytesPerChecksum - 1) / bytesPerChecksum;
    int dataLeft = dataLen;
    
    for (int i = 0; i < numChunks; i++) {
      int len = Math.min(dataLeft, bytesPerChecksum);
      
      checksum.reset();
      checksum.update(buf, curDataOff, len);
      checksum.writeValue(buf, curChecksumOff, false);
      
      curDataOff += len;
      curChecksumOff += checksumSize;
      dataLeft -= len;
    }
  }
}
