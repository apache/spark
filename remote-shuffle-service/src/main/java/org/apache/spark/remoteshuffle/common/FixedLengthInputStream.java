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

package org.apache.spark.remoteshuffle.common;

import org.apache.spark.remoteshuffle.exceptions.RssEndOfStreamException;

import java.io.IOException;
import java.io.InputStream;

public class FixedLengthInputStream extends InputStream {
  private final InputStream stream;
  private final long length;
  private long remaining;

  public FixedLengthInputStream(InputStream stream, long length) {
    this.stream = stream;
    this.length = length;
    this.remaining = length;
  }

  @Override
  public int read() throws IOException {
    if (remaining > 0) {
      int result = stream.read();
      if (result == -1) {
        throw new RssEndOfStreamException(String.format(
            "Unexpected end of stream, expected remaining bytes: %s (total bytes: %s)",
            remaining, length));
      }
      remaining--;
      return result;
    } else {
      return -1;
    }
  }

  @Override
  public int available() {
    return (int) Math.min(Integer.MAX_VALUE, remaining);
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    stream.mark(readlimit);
  }

  @Override
  public synchronized void reset() throws IOException {
    stream.reset();
  }

  @Override
  public boolean markSupported() {
    return stream.markSupported();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }

    if (remaining <= 0) {
      return -1;
    }

    int numBytesToRead = (int) Math.min(len, remaining);
    int result = stream.read(b, off, numBytesToRead);
    if (result == -1) {
      throw new RssEndOfStreamException(String.format(
          "Unexpected end of stream, expected remaining bytes: %s (total bytes: %s)",
          remaining, length));
    } else {
      remaining -= result;
      return result;
    }
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  public long getLength() {
    return length;
  }

  public long getRemaining() {
    return remaining;
  }

  @Override
  public String toString() {
    return "FixedLengthInputStream{" +
        "stream=" + stream +
        ", length=" + length +
        ", remaining=" + remaining +
        '}';
  }
}
