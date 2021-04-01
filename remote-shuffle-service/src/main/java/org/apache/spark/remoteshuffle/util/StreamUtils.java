/*
 * This file is copied from Uber Remote Shuffle Service
 * (https://github.com/uber/RemoteShuffleService) and modified.
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

import org.apache.spark.remoteshuffle.exceptions.RssEndOfStreamException;
import org.apache.spark.remoteshuffle.exceptions.RssStreamReadException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;

public class StreamUtils {

  /***
   * Read given number of bytes from the stream.
   * Return null means end of stream.
   * @param stream
   * @param numBytes
   * @return byte array, returning null means end of stream
   */
  @Nullable
  public static byte[] readBytes(InputStream stream, int numBytes) {
    if (numBytes == 0) {
      return new byte[0];
    }

    byte[] result = new byte[numBytes];
    int readBytes = 0;
    while (readBytes < numBytes) {
      int numBytesToRead = numBytes - readBytes;
      int count;
      try {
        count = stream.read(result, readBytes, numBytesToRead);
      } catch (IOException e) {
        throw new RssStreamReadException("Failed to read data", e);
      }
      if (count == -1) {
        if (readBytes == 0) {
          return null;
        } else {
          throw new RssEndOfStreamException(String.format(
              "Unexpected end of stream, already read bytes: %s, remaining bytes to read: %s ",
              readBytes,
              numBytesToRead));
        }
      }
      readBytes += count;
    }

    return result;
  }
}
