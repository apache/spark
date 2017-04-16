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
package org.apache.hadoop.io.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestBlockDecompressorStream {
  
  private byte[] buf;
  private ByteArrayInputStream bytesIn;
  private ByteArrayOutputStream bytesOut;

  @Test
  public void testRead() throws IOException {
    // compress empty stream
    bytesOut = new ByteArrayOutputStream();
    BlockCompressorStream blockCompressorStream = 
      new BlockCompressorStream(bytesOut, 
          new FakeCompressor(), 1024, 0);
    // close without any write
    blockCompressorStream.close();
    
    // check compressed output 
    buf = bytesOut.toByteArray();
    assertEquals("empty file compressed output size is not 4", 4, buf.length);
    
    // use compressed output as input for decompression
    bytesIn = new ByteArrayInputStream(buf);
    
    // get decompression stream
    BlockDecompressorStream blockDecompressorStream = 
      new BlockDecompressorStream(bytesIn, new FakeDecompressor(), 1024);
    try {
      assertEquals("return value is not -1", 
          -1 , blockDecompressorStream.read());
    } catch (IOException e) {
      fail("unexpected IOException : " + e);
    }
  }
}

/**
 * A fake compressor
 * Its input and output is the same.
 */
class FakeCompressor implements Compressor{

  private boolean finish;
  private boolean finished;
  int nread;
  int nwrite;
  
  byte [] userBuf;
  int userBufOff;
  int userBufLen;
  
  @Override
  public int compress(byte[] b, int off, int len) throws IOException {
    int n = Math.min(len, userBufLen);
    if (userBuf != null && b != null)
      System.arraycopy(userBuf, userBufOff, b, off, n);
    userBufOff += n;
    userBufLen -= n;
    nwrite += n;
    
    if (finish && userBufLen <= 0)
      finished = true;   
        
    return n;
  }

  @Override
  public void end() {
    // nop
  }

  @Override
  public void finish() {
    finish = true;
  }

  @Override
  public boolean finished() {
    return finished;
  }

  @Override
  public long getBytesRead() {
    return nread;
  }

  @Override
  public long getBytesWritten() {
    return nwrite;
  }

  @Override
  public boolean needsInput() {
    return userBufLen <= 0;
  }

  @Override
  public void reset() {
    finish = false;
    finished = false;
    nread = 0;
    nwrite = 0;
    userBuf = null;
    userBufOff = 0;
    userBufLen = 0;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // nop
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    nread += len;
    userBuf = b;
    userBufOff = off;
    userBufLen = len;
  }

  @Override
  public void reinit(Configuration conf) {
    // nop
  }
  
}

/**
 * A fake decompressor, just like FakeCompressor
 * Its input and output is the same.
 */
class FakeDecompressor implements Decompressor {
  
  private boolean finish;
  private boolean finished;
  int nread;
  int nwrite;
  
  byte [] userBuf;
  int userBufOff;
  int userBufLen;

  @Override
  public int decompress(byte[] b, int off, int len) throws IOException {
    int n = Math.min(len, userBufLen);
    if (userBuf != null && b != null)
      System.arraycopy(userBuf, userBufOff, b, off, n);
    userBufOff += n;
    userBufLen -= n;
    nwrite += n;
    
    if (finish && userBufLen <= 0)
      finished = true;
    
    return n;
  }

  @Override
  public void end() {
    // nop
  }

  @Override
  public boolean finished() {
    return finished;
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean needsInput() {
    return userBufLen <= 0;
  }

  @Override
  public void reset() {
    finish = false;
    finished = false;
    nread = 0;
    nwrite = 0;
    userBuf = null;
    userBufOff = 0;
    userBufLen = 0;
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    // nop
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    nread += len;
    userBuf = b;
    userBufOff = off;
    userBufLen = len;
  }
  
}
