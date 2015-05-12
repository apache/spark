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

package org.apache.spark.shuffle.unsafe;

import org.apache.spark.executor.ShuffleWriteMetrics;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Intercepts write calls and tracks total time spent writing.
 */
final class TimeTrackingFileOutputStream extends OutputStream {

  private final ShuffleWriteMetrics writeMetrics;
  private final FileOutputStream outputStream;

  public TimeTrackingFileOutputStream(
      ShuffleWriteMetrics writeMetrics,
      FileOutputStream outputStream) {
    this.writeMetrics = writeMetrics;
    this.outputStream = outputStream;
  }

  @Override
  public void write(int b) throws IOException {
    final long startTime = System.nanoTime();
    outputStream.write(b);
    writeMetrics.incShuffleWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void write(byte[] b) throws IOException {
    final long startTime = System.nanoTime();
    outputStream.write(b);
    writeMetrics.incShuffleWriteTime(System.nanoTime() - startTime);  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    final long startTime = System.nanoTime();
    outputStream.write(b, off, len);
    writeMetrics.incShuffleWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
