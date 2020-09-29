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

package org.apache.spark.network.shuffle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.spark.network.shuffle.protocol.BlockTransferMessage;
import org.apache.spark.network.shuffle.protocol.PushBlockStream;

/**
 * A special exception type that would decode the encoded {@link PushBlockStream} from the
 * exception String. This complements the encoding logic in
 * {@link org.apache.spark.network.server.TransportRequestHandler}.
 */
public class BlockPushException extends RuntimeException {
  private PushBlockStream header;

  /**
   * String constant used for generating exception messages indicating a block to be merged
   * arrives too late on the server side, and also for later checking such exceptions on the
   * client side. When we get a block push failure because of the block arrives too late, we
   * will not retry pushing the block nor log the exception on the client side.
   */
  public static final String TOO_LATE_MESSAGE_SUFFIX =
      "received after merged shuffle is finalized";

  /**
   * String constant used for generating exception messages indicating the server couldn't
   * append a block after all available attempts due to collision with other blocks belonging
   * to the same shuffle partition, and also for later checking such exceptions on the client
   * side. When we get a block push failure because of the block couldn't be written due to
   * this reason, we will not log the exception on the client side.
   */
  public static final String COULD_NOT_FIND_OPPORTUNITY_MSG_PREFIX =
      "Couldn't find an opportunity to write block";

  private BlockPushException(PushBlockStream header, String message) {
    super(message);
    this.header = header;
  }

  public static BlockPushException decodeException(String message) {
    // Use ISO_8859_1 encoding instead of UTF_8. UTF_8 will change the byte content
    // for bytes larger than 127. This would render incorrect result when encoding
    // decoding the index inside the PushBlockStream message.
    ByteBuffer rawBuffer = ByteBuffer.wrap(message.getBytes(StandardCharsets.ISO_8859_1));
    try {
      BlockTransferMessage msgObj = BlockTransferMessage.Decoder.fromByteBuffer(rawBuffer);
      if (msgObj instanceof PushBlockStream) {
        PushBlockStream header = (PushBlockStream) msgObj;
        // When decoding the header, the rawBuffer's position is not updated since it was
        // consumed via netty's ByteBuf. Updating the rawBuffer's position here to retrieve
        // the remaining exception message.
        ByteBuffer remainingBuffer = (ByteBuffer) rawBuffer.position(rawBuffer.position()
            + header.encodedLength() + 1);
        return new BlockPushException(header,
            StandardCharsets.UTF_8.decode(remainingBuffer).toString());
      } else {
        throw new UnsupportedOperationException(String.format("Cannot decode the header. "
            + "Expected PushBlockStream but got %s instead", msgObj.getClass().getSimpleName()));
      }
    } catch (Exception e) {
      return new BlockPushException(null, message);
    }
  }

  public PushBlockStream getHeader() {
    return header;
  }
}
