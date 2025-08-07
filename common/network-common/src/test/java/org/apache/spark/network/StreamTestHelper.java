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

package org.apache.spark.network;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.buffer.NioManagedBuffer;
import org.apache.spark.network.util.JavaUtils;
import org.apache.spark.network.util.TransportConf;

class StreamTestHelper {
  static final String[] STREAMS = { "largeBuffer", "smallBuffer", "emptyBuffer", "file" };

  final File testFile;
  final File tempDir;

  final ByteBuffer emptyBuffer;
  final ByteBuffer smallBuffer;
  final ByteBuffer largeBuffer;

  private static ByteBuffer createBuffer(int bufSize) {
    ByteBuffer buf = ByteBuffer.allocate(bufSize);
    for (int i = 0; i < bufSize; i ++) {
      buf.put((byte) i);
    }
    buf.flip();
    return buf;
  }

  StreamTestHelper() throws Exception {
    tempDir = JavaUtils.createDirectory(System.getProperty("java.io.tmpdir"), "spark");
    emptyBuffer = createBuffer(0);
    smallBuffer = createBuffer(100);
    largeBuffer = createBuffer(100000);

    testFile = File.createTempFile("stream-test-file", "txt", tempDir);
    try (FileOutputStream fp = new FileOutputStream(testFile)) {
      Random rnd = new Random();
      for (int i = 0; i < 512; i++) {
        byte[] fileContent = new byte[1024];
        rnd.nextBytes(fileContent);
        fp.write(fileContent);
      }
    }
  }

  public ByteBuffer srcBuffer(String name) {
    return switch (name) {
      case "largeBuffer" -> largeBuffer;
      case "smallBuffer" -> smallBuffer;
      case "emptyBuffer" -> emptyBuffer;
      default -> throw new IllegalArgumentException("Invalid stream: " + name);
    };
  }

  public ManagedBuffer openStream(TransportConf conf, String streamId) {
    if ("file".equals(streamId)) {
      return new FileSegmentManagedBuffer(conf, testFile, 0, testFile.length());
    }
    return new NioManagedBuffer(srcBuffer(streamId));
  }

  void cleanup() {
    if (tempDir != null) {
      try {
        JavaUtils.deleteRecursively(tempDir);
      } catch (Exception io) {
        throw new RuntimeException(io);
      }
    }
  }
}
