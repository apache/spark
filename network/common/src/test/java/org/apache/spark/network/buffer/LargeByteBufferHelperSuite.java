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
package org.apache.spark.network.buffer;

import org.junit.Test;

import java.io.*;
import java.nio.channels.FileChannel;

import static org.junit.Assert.*;

public class LargeByteBufferHelperSuite {

  @Test
  public void testMapFile() throws IOException {
    File testFile = File.createTempFile("large-byte-buffer-test", ".bin");
    testFile.deleteOnExit();
    OutputStream out = new FileOutputStream(testFile);
    byte[] buffer = new byte[1 << 16];
    long len = 3L << 30;
    assertTrue(len > Integer.MAX_VALUE);  // its 1.5x Integer.MAX_VALUE, just a sanity check
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = (byte) i;
    }
    for (int i = 0; i < len / buffer.length; i++) {
      out.write(buffer);
    }
    out.close();

    FileChannel in = new FileInputStream(testFile).getChannel();

    //fail quickly on bad bounds
    try {
      LargeByteBufferHelper.mapFile(in, FileChannel.MapMode.READ_ONLY, 0, len + 1);
      fail("expected exception");
    } catch (IOException ioe) {
    }
    try {
      LargeByteBufferHelper.mapFile(in, FileChannel.MapMode.READ_ONLY, -1, 10);
      fail("expected exception");
    } catch (IllegalArgumentException iae) {
    }

    //now try to read from the buffer
    LargeByteBuffer buf = LargeByteBufferHelper.mapFile(in, FileChannel.MapMode.READ_ONLY, 0, len);
    assertEquals(len, buf.size());
    byte[] read = new byte[buffer.length];
    for (int i = 0; i < len / buffer.length; i++) {
      buf.get(read, 0, buffer.length);
      // assertArrayEquals() is really slow
      for (int j = 0; j < buffer.length; j++) {
        if (read[j] != (byte)(j))
          fail("bad byte at (i,j) = (" + i + "," + j + ")");
      }
    }
  }
}
