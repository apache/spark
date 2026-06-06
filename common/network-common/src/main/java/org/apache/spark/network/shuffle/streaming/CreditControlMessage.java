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

package org.apache.spark.network.shuffle.streaming;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

/**
 * Reader → writer control message.
 *
 * Current function: serves as a connection-establishment and per-consumption "ready"
 * signal. The writer uses receipt of any CreditControlMessage as the trigger that the
 * reader is ready to receive, and does not act on the numeric value of {@link
 * #numMessages}. Backpressure today is handled at the TCP layer via channel autoRead,
 * not via this message.
 *
 * Future function: this message is reserved as the carrier for a future credit-based
 * flow-control extension. When that extension lands, {@link #numMessages} will carry
 * the number of additional DataMessages the writer may send beyond any
 * previously-granted credit (i.e., a credit-grant delta).
 */
public final class CreditControlMessage extends StreamingShuffleMessage {
  public final int shuffleWriterId;
  public final int shuffleReaderId;

  /**
   * In the current protocol revision the writer ignores this value and treats any
   * CreditControlMessage as a "reader is ready" signal; senders should pass 1.
   *
   * Reserved for the future credit-based flow-control extension, in which this field
   * will carry the number of additional DataMessages the writer may send beyond any
   * previously-granted credit.
   */
  public final int numMessages;

  public CreditControlMessage(int shuffleWriterId, int shuffleReaderId, int numMessages) {
    this.shuffleWriterId = shuffleWriterId;
    this.shuffleReaderId = shuffleReaderId;
    this.numMessages = numMessages;
  }

  @Override
  public StreamingShuffleMessageType messageType() {
    return StreamingShuffleMessageType.CREDIT_CONTROL_MESSAGE;
  }

  @Override
  public int headerLength() {
    // 4 bytes for the shuffle writer ID, 4 bytes for the shuffle reader ID,
    // 4 bytes for the number of messages
    return super.headerLength() + 12;
  }

  @Override
  public void encode(CompositeByteBuf buf) {
    super.encode(buf);

    // Write the shuffle writer ID
    buf.writeInt(shuffleWriterId);
    // Write the shuffle reader ID
    buf.writeInt(shuffleReaderId);
    // Write the number of messages
    buf.writeInt(numMessages);
  }

  public static CreditControlMessage decode(ByteBuf buf) {
    // Read the shuffle writer ID
    int shuffleWriterId = buf.readInt();
    // Read the shuffle reader ID
    int shuffleReaderId = buf.readInt();
    // Read the number of messages
    int numMessages = buf.readInt();

    return new CreditControlMessage(shuffleWriterId, shuffleReaderId, numMessages);
  }
}
