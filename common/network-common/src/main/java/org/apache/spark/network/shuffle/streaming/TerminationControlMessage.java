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

public final class TerminationControlMessage extends StreamingShuffleMessage {
  public final int shuffleWriterId;
  public final int shuffleReaderId;

  public TerminationControlMessage(int shuffleWriterId, int shuffleReaderId) {
    this.shuffleWriterId = shuffleWriterId;
    this.shuffleReaderId = shuffleReaderId;
  }

  @Override
  public StreamingShuffleMessageType messageType() {
    return StreamingShuffleMessageType.TERMINATION_CONTROL_MESSAGE;
  }

  @Override
  public int headerLength() {
    // 4 bytes for the shuffle writer ID, 4 bytes for the shuffle reader ID
    return super.headerLength() + 8;
  }

  @Override
  public void encode(CompositeByteBuf buf) {
    super.encode(buf);

    // Write the shuffle writer ID
    buf.writeInt(shuffleWriterId);
    // Write the shuffle reader ID
    buf.writeInt(shuffleReaderId);
  }

  public static TerminationControlMessage decode(ByteBuf buf) {
    // Read the shuffle writer ID
    int shuffleWriterId = buf.readInt();
    // Read the shuffle reader ID
    int shuffleReaderId = buf.readInt();

    return new TerminationControlMessage(shuffleWriterId, shuffleReaderId);
  }
}
