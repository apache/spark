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

package org.apache.spark.shuffle.example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;

public class JsonShufflePartitionWriter implements ShufflePartitionWriter {

  private final int partitionId;
  private final Path outputPath;
  private final ByteArrayOutputStream buffer;
  private final ObjectMapper objectMapper;
  private long bytesWritten;
  private boolean closed;

  public JsonShufflePartitionWriter(int partitionId, Path outputPath, ObjectMapper objectMapper) {
    this.partitionId = partitionId;
    this.outputPath = outputPath;
    this.buffer = new ByteArrayOutputStream();
    this.objectMapper = objectMapper;
    this.bytesWritten = 0;
    this.closed = false;
  }

  @Override
  public OutputStream openStream() throws IOException {
    if (closed) {
      throw new IllegalStateException("Cannot open stream after partition writer is closed.");
    }
    return new OutputStream() {
      private void checkOpen() {
        if (closed) {
          throw new IllegalStateException("Cannot write to a closed partition writer.");
        }
      }

      @Override
      public void write(int b) throws IOException {
        checkOpen();
        buffer.write(b);
        bytesWritten++;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        checkOpen();
        buffer.write(b, off, len);
        bytesWritten += len;
      }

      @Override
      public void write(byte[] b) throws IOException {
        checkOpen();
        buffer.write(b);
        bytesWritten += b.length;
      }

      @Override
      public void close() {
        closed = true;
      }
    };
  }

  private void checkOpen() {
    if (closed) {
      throw new IllegalStateException("Cannot write to a closed partition writer.");
    }
  }

  @Override
  public long getNumBytesWritten() {
    return bytesWritten;
  }

  public void writeJson() throws IOException {
    if (!closed) {
      throw new IllegalStateException("Partition writer must be closed before writing JSON.");
    }

    Map<String, Object> payload = new HashMap<>();
    payload.put("partitionId", partitionId);
    payload.put("bytesWritten", bytesWritten);
    payload.put("timestamp", System.currentTimeMillis());
    payload.put("data", Base64.getEncoder().encodeToString(buffer.toByteArray()));

    Files.createDirectories(outputPath.getParent());
    byte[] jsonBytes = objectMapper.writeValueAsBytes(payload);
    Files.write(outputPath, jsonBytes);
  }
}
