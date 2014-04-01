/*
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

package org.apache.hadoop.mapred;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;
/**
 * A checksum input stream, used for IFiles.
 * Used to validate the checksum of files created by {@link IFileOutputStream}. 
 */

class IFileInputStream extends InputStream {
  
  private final InputStream in; //The input stream to be verified for checksum. 
  private final long length; //The total length of the input file
  private final long dataLength;
  private DataChecksum sum;
  private long currentOffset = 0;
  private final byte b[] = new byte[1];
  private byte csum[] = null;
  private int checksumSize;
  
  /**
   * Create a checksum input stream that reads
   * @param in The input stream to be verified for checksum.
   * @param len The length of the input stream including checksum bytes.
   */
  public IFileInputStream(InputStream in, long len) {
    this.in = in;
    sum = DataChecksum.newDataChecksum(DataChecksum.CHECKSUM_CRC32, 
        Integer.MAX_VALUE);
    checksumSize = sum.getChecksumSize();
    length = len;
    dataLength = length - checksumSize;
  }

  /**
   * Close the input stream. Note that we need to read to the end of the
   * stream to validate the checksum.
   */
  @Override
  public void close() throws IOException {
    if (currentOffset < dataLength) {
      byte[] t = new byte[Math.min((int)
            (Integer.MAX_VALUE & (dataLength - currentOffset)), 32 * 1024)];
      while (currentOffset < dataLength) {
        int n = read(t, 0, t.length);
        if (0 == n) {
          throw new EOFException("Could not validate checksum");
        }
      }
    }
    in.close();
  }
  
  @Override
  public long skip(long n) throws IOException {
   throw new IOException("Skip not supported for IFileInputStream");
  }
  
  public long getPosition() {
    return (currentOffset >= dataLength) ? dataLength : currentOffset;
  }
  
  public long getSize() {
    return checksumSize;
  }
  
  /**
   * Read bytes from the stream.
   * At EOF, checksum is validated, but the checksum
   * bytes are not passed back in the buffer. 
   */
  public int read(byte[] b, int off, int len) throws IOException {

    if (currentOffset >= dataLength) {
      return -1;
    }
    
    return doRead(b,off,len);
  }

  /**
   * Read bytes from the stream.
   * At EOF, checksum is validated and sent back
   * as the last four bytes of the buffer. The caller should handle
   * these bytes appropriately
   */
  public int readWithChecksum(byte[] b, int off, int len) throws IOException {

    if (currentOffset == length) {
      return -1;
    }
    else if (currentOffset >= dataLength) {
      // If the previous read drained off all the data, then just return
      // the checksum now. Note that checksum validation would have 
      // happened in the earlier read
      int lenToCopy = (int) (checksumSize - (currentOffset - dataLength));
      if (len < lenToCopy) {
        lenToCopy = len;
      }
      System.arraycopy(csum, (int) (currentOffset - dataLength), b, off, 
          lenToCopy);
      currentOffset += lenToCopy;
      return lenToCopy;
    }

    int bytesRead = doRead(b,off,len);

    if (currentOffset == dataLength) {
      if (len >= bytesRead + checksumSize) {
        System.arraycopy(csum, 0, b, off + bytesRead, checksumSize);
        bytesRead += checksumSize;
        currentOffset += checksumSize;
      }
    }
    return bytesRead;
  }

  private int doRead(byte[]b, int off, int len) throws IOException {
    
    // If we are trying to read past the end of data, just read
    // the left over data
    if (currentOffset + len > dataLength) {
      len = (int) dataLength - (int)currentOffset;
    }
    
    int bytesRead = in.read(b, off, len);

    if (bytesRead < 0) {
      throw new ChecksumException("Checksum Error", 0);
    }
    
    sum.update(b,off,bytesRead);

    currentOffset += bytesRead;
    
    if (currentOffset == dataLength) {
      // The last four bytes are checksum. Strip them and verify
      csum = new byte[checksumSize];
      IOUtils.readFully(in, csum, 0, checksumSize);
      if (!sum.compare(csum, 0)) {
        throw new ChecksumException("Checksum Error", 0);
      }
    }
    return bytesRead;
  }


  @Override
  public int read() throws IOException {    
    b[0] = 0;
    int l = read(b,0,1);
    if (l < 0)  return l;
    
    // Upgrade the b[0] to an int so as not to misinterpret the
    // first bit of the byte as a sign bit
    int result = 0xFF & b[0];
    return result;
  }

  public byte[] getChecksum() {
    return csum;
  }
}
