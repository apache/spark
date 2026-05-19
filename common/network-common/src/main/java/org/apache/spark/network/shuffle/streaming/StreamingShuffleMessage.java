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
 * Base class for messages sent between the streaming shuffle writers (usually mappers) and
 * readers (usually reducers).
 *
 * To prevent memory leaks, streaming shuffle programmers should always abide by the following
 * principles:
 *
 *  1. If you create a buffer via ByteBufAllocator, you must explicitly release it.
 *  2. If you create a new StreamingShuffleMessage, you must call .release() on it.
 *
 * To make these rules work out, implementations of StreamingShuffleMessage should abide
 * by the following rules:
 *
 *  1. StreamingShuffleMessages should *not* modify the refcount of ByteBufs passed to them
 *     during encoding. For message implementations without ByteBufs, this isn't a concern.
 *     But for messages that have ByteBufs (e.g. DataMessage), the encoding method will likely
 *     call compositeByteBuf.addComponent(), which transfers ownership of the ByteBuf to the
 *     CompositeByteBuf and decrements the refcount of the ByteBuf. So that the caller can
 *     *always* follow rule 1 above, the ByteBuf should be retained before being passed to the
 *     CompositeByteBuf; if this is not done, the refcount of the ByteBuf after leaving
 *     encode() will be 0, and if the caller follows rule 1, they will try to decrement an
 *     already 0 refcount. See DataMessage for an example of how to do this properly.
 *  2. If StreamingShuffleMessages keep a reference the ByteBufs passed to them during
 *     decoding, they should increment the refcount of that ByteBuf, and assign it to
 *     ownedBuf. This is so that resources get cleaned up when callers follow rule 2 above,
 *     i.e. call .release() on the StreamingShuffleMessage. See DataMessage for an example of
 *     how to do this properly.
 */
public abstract sealed class StreamingShuffleMessage
    permits CreditControlMessage, DataMessage, TerminationAckMessage, TerminationControlMessage {
    protected ByteBuf ownedBuf = null;
    private Runnable releaseCallback = null;

    // To prevent any duplicate/out of order/missing messages, each writer will track the current
    // max sequence number that has been sent to each reader. Similarly, each reader will track
    // the latest sequence number it has received from each writer. Upon receiving a new message
    // from any writer, reader will check if the sequence number is expected. When all finish, the
    // reader will send TerminationAckMessage to the writer with the max sequence number that has
    // been received, and the writer will check if the latest sequence recorded matches it.

    // Thus the sequence number is valid for the following message types:
    // 1. all message types from a writer to a reader. To make sure that the reader
    // receive all the messages sent by writer in order without missing or duplicate any.
    // 2. TerminationAckMessage from a reader to a writer. To make sure at the end of the
    // shuffle, the reader receives the same number of messages that the writer has sent.
    // Essentially, other message types from reader to writer won't have a valid sequence number.
    private long seqNum;
    public void setSeqNum(long seqNum) {
        this.seqNum = seqNum;
    }
    public long getSeqNum() { return seqNum; }

    /** Returns the type of this message. */
    public abstract StreamingShuffleMessageType messageType();

    /** Encodes the current message into the provided ByteBuf. */
    public void encode(CompositeByteBuf buf) {
        buf.writeInt(messageType().id());
        buf.writeLong(seqNum);
    }

    public int headerLength() {
        // 4 bytes for message type, 8 bytes for the sequence number
        return 12;
    }

    public void setReleaseCallback(Runnable releaseCallback) {
        this.releaseCallback = releaseCallback;
    }

    /**
     * Releases any resources associated with this message.
     * In VERY RARE cases when the task fails unexpectedly, this method may be called twice.
     * This method is idempotent — a second call on the same thread is a no-op — but it is
     * NOT thread-safe.
     */
    public void release() {
        if (ownedBuf != null) {
            ownedBuf.release();
            ownedBuf = null;
        }
        if (releaseCallback != null) {
            releaseCallback.run();
            releaseCallback = null;
        }
    }

    public static StreamingShuffleMessage decode(ByteBuf message) {
        StreamingShuffleMessageType messageType =
            StreamingShuffleMessageType.decode(message.readInt());
        long seqNum = message.readLong();

        // Switch expression over the enum is exhaustive; the compiler enforces that every
        // case is handled, so any future StreamingShuffleMessageType added to the enum will
        // cause this method to fail compilation until the corresponding case is added here.
        StreamingShuffleMessage shuffleMessage = switch (messageType) {
            case DATA_MESSAGE_UNSAFE_ROW -> DataMessage.decode(message);
            case CREDIT_CONTROL_MESSAGE -> CreditControlMessage.decode(message);
            case TERMINATION_CONTROL_MESSAGE -> TerminationControlMessage.decode(message);
            case TERMINATION_ACK_MESSAGE -> TerminationAckMessage.decode(message);
        };
        shuffleMessage.setSeqNum(seqNum);

        return shuffleMessage;
    }

}
