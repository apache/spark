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
import java.io.OutputStream;

import org.apache.spark.annotation.Private;
import org.apache.spark.executor.ShuffleWriteMetrics;

/**
 * Intercepts write calls and tracks total time spent writing in order to update shuffle write
 * metrics. Not thread safe.
 */
@Private
public final class TimeTrackingOutputStream extends OutputStream {

  private final ShuffleWriteMetrics writeMetrics;
  private final OutputStream outputStream;

  public TimeTrackingOutputStream(ShuffleWriteMetrics writeMetrics, OutputStream outputStream) {
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
    writeMetrics.incShuffleWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    final long startTime = System.nanoTime();
    outputStream.write(b, off, len);
    writeMetrics.incShuffleWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void flush() throws IOException {
    final long startTime = System.nanoTime();
    outputStream.flush();
    writeMetrics.incShuffleWriteTime(System.nanoTime() - startTime);
  }

  @Override
  public void close() throws IOException {
    final long startTime = System.nanoTime();
    outputStream.close();
    writeMetrics.incShuffleWriteTime(System.nanoTime() - startTime);
  }
}
