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

package org.apache.spark.storage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * A {@link ManagedBuffer} backed by a file using Hadoop FileSystem.
 * This buffer creates an input stream with a 64MB buffer size for efficient reading.
 * Note: This implementation throws UnsupportedOperationException for methods that
 * require loading the entire file into memory (nioByteBuffer, convertToNetty, convertToNettyForSsl)
 * as files can be very large and loading them entirely into memory is not practical.
 */
public class FileSystemManagedBuffer extends ManagedBuffer {
  private int bufferSize; // 64MB buffer size
  private final Path filePath;
  private final long fileSize;
  private final Configuration hadoopConf;

  public FileSystemManagedBuffer(Path filePath, Configuration hadoopConf) throws IOException {
    this.filePath = filePath;
    this.hadoopConf = hadoopConf;
    // Get file size using FileSystem.newInstance to avoid cached dependencies
    FileSystem fileSystem = FileSystem.newInstance(filePath.toUri(), hadoopConf);
    try {
      this.fileSize = fileSystem.getFileStatus(filePath).getLen();
    } finally {
      fileSystem.close();
    }
    bufferSize = 64;
  }

  public FileSystemManagedBuffer(Path filePath, Configuration hadoopConf, int bufferSize)
          throws IOException {
    this(filePath, hadoopConf);
    this.bufferSize = bufferSize;
  }

  @Override
  public long size() {
    return fileSize;
  }

  @Override
  public ByteBuffer nioByteBuffer() throws IOException {
    throw new UnsupportedOperationException(
      "FileSystemManagedBuffer does not support nioByteBuffer() as it would require loading " +
      "the entire file into memory, which is not practical for large files. " +
      "Use createInputStream() instead.");
  }

  @Override
  public InputStream createInputStream() throws IOException {
    // Create a new FileSystem instance to avoid cached dependencies
    // and create a buffered input stream with 64MB buffer size for efficient reading
    FileSystem fileSystem = FileSystem.newInstance(filePath.toUri(), hadoopConf);
    return fileSystem.open(filePath, bufferSize * 1024 * 1024);
  }

  @Override
  public ManagedBuffer retain() {
    // FileSystemManagedBuffer doesn't use reference counting, so just return this
    return this;
  }

  @Override
  public ManagedBuffer release() {
    // FileSystemManagedBuffer doesn't use reference counting, so just return this
    return this;
  }

  @Override
  public Object convertToNetty() {
    throw new UnsupportedOperationException(
      "FileSystemManagedBuffer does not support convertToNetty() as it would require loading " +
      "the entire file into memory, which is not practical for large files. " +
      "Use createInputStream() instead.");
  }

  @Override
  public Object convertToNettyForSsl() {
    throw new UnsupportedOperationException(
      "FileSystemManagedBuffer does not support convertToNettyForSsl()" +
              " as it would require loading " +
      "the entire file into memory, which is not practical for large files. " +
      "Use createInputStream() instead.");
  }

  @Override
  public String toString() {
    return "FileSegmentManagedBuffer[file=" + filePath + ",length=" + fileSize + "]";
  }
}
