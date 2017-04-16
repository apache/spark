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
package org.apache.hadoop.fs;

import junit.framework.TestCase;
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

/**
 * test for the input truncation bug when mark/reset is used.
 * HADOOP-1489
 */
public class TestTruncatedInputBug extends TestCase {
  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');
  
  private void writeFile(FileSystem fileSys, 
                         Path name, int nBytesToWrite) 
    throws IOException {
    DataOutputStream out = fileSys.create(name);
    for (int i = 0; i < nBytesToWrite; ++i) {
      out.writeByte(0);
    }
    out.close();
  }
  
  /**
   * When mark() is used on BufferedInputStream, the request
   * size on the checksum file system can be small.  However,
   * checksum file system currently depends on the request size
   * >= bytesPerSum to work properly.
   */
  public void testTruncatedInputBug() throws IOException {
    final int ioBufSize = 512;
    final int fileSize = ioBufSize*4;
    int filePos = 0;

    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", ioBufSize);
    FileSystem fileSys = FileSystem.getLocal(conf);

    try {
      // First create a test input file.
      Path testFile = new Path(TEST_ROOT_DIR, "HADOOP-1489");
      writeFile(fileSys, testFile, fileSize);
      assertTrue(fileSys.exists(testFile));
      assertTrue(fileSys.getLength(testFile) == fileSize);

      // Now read the file for ioBufSize bytes
      FSDataInputStream in = fileSys.open(testFile, ioBufSize);
      // seek beyond data buffered by open
      filePos += ioBufSize * 2 + (ioBufSize - 10);  
      in.seek(filePos);

      // read 4 more bytes before marking
      for (int i = 0; i < 4; ++i) {  
        if (in.read() == -1) {
          break;
        }
        ++filePos;
      }

      // Now set mark() to trigger the bug
      // NOTE: in the fixed code, mark() does nothing (not supported) and
      //   hence won't trigger this bug.
      in.mark(1);
      System.out.println("MARKED");
      
      // Try to read the rest
      while (filePos < fileSize) {
        if (in.read() == -1) {
          break;
        }
        ++filePos;
      }
      in.close();

      System.out.println("Read " + filePos + " bytes."
                         + " file size=" + fileSize);
      assertTrue(filePos == fileSize);

    } finally {
      try {
        fileSys.close();
      } catch (Exception e) {
        // noop
      }
    }
  }  // end testTruncatedInputBug
}
