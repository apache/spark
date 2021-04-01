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

package org.apache.spark.remoteshuffle.tools;

import org.apache.spark.remoteshuffle.clients.DataBlockSyncWriteClient;
import org.apache.spark.remoteshuffle.common.ServerDetail;
import org.apache.spark.remoteshuffle.messages.ConnectUploadResponse;
import org.apache.spark.remoteshuffle.util.StreamUtils;

import javax.annotation.Nullable;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TestUtils {
  /**
   * Serialize a string with putting length first and then bytes.
   *
   * @param str string value to serialize.
   * @return Serialized bytes.
   */
  public static byte[] serializeString(String str) {
    byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
    ByteBuffer buffer = ByteBuffer.allocate(4 + bytes.length);
    buffer.putInt(bytes.length);
    buffer.put(bytes);
    return buffer.array();
  }

  /***
   * Read string from a stream, returning null means end of stream.
   * @param stream input stream.
   * @return the string value.
   */
  @Nullable
  public static String readString(InputStream stream) {
    byte[] bytes = StreamUtils.readBytes(stream, 4);
    if (bytes == null) {
      return null;
    }

    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int len = buffer.getInt();
    if (len < 0) {
      throw new RuntimeException("Invalid string length in stream");
    }

    bytes = StreamUtils.readBytes(stream, len);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static ServerDetail getServerDetail(String host, int port) {
    int networkTimeoutMillis = 60000;
    boolean finishUploadAck = true;
    try (DataBlockSyncWriteClient writeClient = new DataBlockSyncWriteClient(
        host,
        port,
        networkTimeoutMillis,
        finishUploadAck,
        "user1",
        "app1",
        "appAttempt1"
    )) {
      ConnectUploadResponse connectUploadResponse = writeClient.connect();
      return new ServerDetail(connectUploadResponse.getServerId(),
          String.format("%s:%s", host, port));
    }
  }
}
