/*
 * This file is copied from Uber Remote Shuffle Service
(https://github.com/uber/RemoteShuffleService) and modified.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.remoteshuffle.util;

import java.io.IOException;
import java.io.OutputStream;

public class CountedOutputStream extends OutputStream {
  private long writtenBytes = 0;

  private OutputStream underlyingStream;

  public CountedOutputStream(OutputStream underlyingStream) {
    this.underlyingStream = underlyingStream;
  }

  @Override
  public synchronized void write(int i) throws IOException {
    underlyingStream.write(i);
    writtenBytes++;
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    underlyingStream.write(b, off, len);
    writtenBytes += len;
  }

  @Override
  public synchronized void flush() throws IOException {
    underlyingStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    underlyingStream.close();
  }

  public synchronized long getWrittenBytes() {
    return writtenBytes;
  }
}
