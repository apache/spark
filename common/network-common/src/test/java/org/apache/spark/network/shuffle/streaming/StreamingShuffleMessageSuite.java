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
import io.netty.buffer.Unpooled;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link StreamingShuffleMessage} encode/decode round-trips and
 * {@link ShuffleChecksum}.
 */
public class StreamingShuffleMessageSuite {

  private static final long SEQ_NUM = 42L;

  // ---- helpers ---------------------------------------------------------------

  private ByteBuf encodeAndSlice(StreamingShuffleMessage msg) {
    msg.setSeqNum(SEQ_NUM);
    CompositeByteBuf buf = Unpooled.compositeBuffer();
    buf.capacity(msg.headerLength());
    msg.encode(buf);
    msg.release();
    // Return a copy so we can freely advance the reader index
    ByteBuf copy = Unpooled.buffer(buf.readableBytes());
    copy.writeBytes(buf);
    buf.release();
    return copy;
  }

  // ---- CreditControlMessage --------------------------------------------------

  @Test
  public void testCreditControlRoundTrip() {
    CreditControlMessage original = new CreditControlMessage(3, 7, 5);
    ByteBuf encoded = encodeAndSlice(original);
    try {
      StreamingShuffleMessage decoded = StreamingShuffleMessage.decode(encoded);
      assertInstanceOf(CreditControlMessage.class, decoded);
      CreditControlMessage credit = (CreditControlMessage) decoded;
      assertEquals(SEQ_NUM, credit.getSeqNum());
      assertEquals(3, credit.shuffleWriterId);
      assertEquals(7, credit.shuffleReaderId);
      assertEquals(5, credit.numMessages);
    } finally {
      encoded.release();
    }
  }

  // ---- TerminationControlMessage ---------------------------------------------

  @Test
  public void testTerminationControlRoundTrip() {
    TerminationControlMessage original = new TerminationControlMessage(1, 2);
    ByteBuf encoded = encodeAndSlice(original);
    try {
      StreamingShuffleMessage decoded = StreamingShuffleMessage.decode(encoded);
      assertInstanceOf(TerminationControlMessage.class, decoded);
      TerminationControlMessage term = (TerminationControlMessage) decoded;
      assertEquals(SEQ_NUM, term.getSeqNum());
      assertEquals(1, term.shuffleWriterId);
      assertEquals(2, term.shuffleReaderId);
    } finally {
      encoded.release();
    }
  }

  // ---- TerminationAckMessage -------------------------------------------------

  @Test
  public void testTerminationAckRoundTrip() {
    TerminationAckMessage original = new TerminationAckMessage(4, 8);
    ByteBuf encoded = encodeAndSlice(original);
    try {
      StreamingShuffleMessage decoded = StreamingShuffleMessage.decode(encoded);
      assertInstanceOf(TerminationAckMessage.class, decoded);
      TerminationAckMessage ack = (TerminationAckMessage) decoded;
      assertEquals(SEQ_NUM, ack.getSeqNum());
      assertEquals(4, ack.shuffleWriterId);
      assertEquals(8, ack.shuffleReaderId);
    } finally {
      encoded.release();
    }
  }

  // ---- DataMessage -----------------------------------------------------------

  @Test
  public void testDataMessageRoundTrip() {
    byte[] payload = "hello streaming shuffle".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    long checksum = 0xDEADBEEFL;

    DataMessage original = new DataMessage(2, 5, payload.length, payloadBuf, checksum);
    ByteBuf encoded = encodeAndSlice(original);
    payloadBuf.release();

    try {
      StreamingShuffleMessage decoded = StreamingShuffleMessage.decode(encoded);
      assertInstanceOf(DataMessage.class, decoded);
      DataMessage dm = (DataMessage) decoded;
      assertEquals(SEQ_NUM, dm.getSeqNum());
      assertEquals(2, dm.shuffleWriterId);
      assertEquals(5, dm.shuffleReaderId);
      assertEquals(payload.length, dm.dataSize);
      assertEquals(checksum, dm.checksum);

      ByteBuf recordData = dm.getRecordData();
      byte[] out = new byte[payload.length];
      recordData.readBytes(out);
      assertArrayEquals(payload, out);
      dm.release();
    } finally {
      encoded.release();
    }
  }

  @Test
  public void testReleaseIsIdempotent() {
    byte[] payload = "hi".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    // Constructor calls data.retain(), so refcount is now 2 (1 original + 1 retain).
    DataMessage msg = new DataMessage(0, 0, payload.length, payloadBuf, 0L);
    assertEquals(2, payloadBuf.refCnt());

    msg.release();
    assertEquals(1, payloadBuf.refCnt());

    // Second release should be a no-op — refcount must not drop further or throw.
    msg.release();
    assertEquals(1, payloadBuf.refCnt());

    payloadBuf.release();
  }

  @Test
  public void testReleaseCallbackRunsExactlyOnce() {
    byte[] payload = "hi".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    DataMessage msg = new DataMessage(0, 0, payload.length, payloadBuf, 0L);

    java.util.concurrent.atomic.AtomicInteger callbackInvocations =
        new java.util.concurrent.atomic.AtomicInteger(0);
    msg.setReleaseCallback(callbackInvocations::incrementAndGet);

    msg.release();
    assertEquals(1, callbackInvocations.get());

    // Second release: idempotent — callback must NOT fire again.
    msg.release();
    assertEquals(1, callbackInvocations.get());

    payloadBuf.release();
  }

  @Test
  public void testDataMessageRejectsDataSizeLargerThanReadableBytes() {
    byte[] payload = "hello".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    try {
      // dataSize (10) > data.readableBytes() (5)
      assertThrows(IllegalArgumentException.class,
          () -> new DataMessage(0, 0, payload.length + 5, payloadBuf, 0L));
    } finally {
      payloadBuf.release();
    }
  }

  @Test
  public void testDataMessageRejectsDataSizeSmallerThanReadableBytes() {
    byte[] payload = "hello".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    try {
      // dataSize (3) < data.readableBytes() (5)
      assertThrows(IllegalArgumentException.class,
          () -> new DataMessage(0, 0, payload.length - 2, payloadBuf, 0L));
    } finally {
      payloadBuf.release();
    }
  }

  @Test
  public void testDataMessageRejectsNegativeDataSize() {
    byte[] payload = "hello".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    try {
      assertThrows(IllegalArgumentException.class,
          () -> new DataMessage(0, 0, -1, payloadBuf, 0L));
    } finally {
      payloadBuf.release();
    }
  }

  @Test
  public void testDataMessageDecodeRejectsTrailingBytes() {
    // Encode a valid DataMessage, then append junk bytes to the encoded buffer.
    // decode() should reject because dataSize != message.readableBytes() after the header.
    byte[] payload = "hello".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    DataMessage original = new DataMessage(1, 2, payload.length, payloadBuf, 0L);
    original.setSeqNum(SEQ_NUM);
    CompositeByteBuf buf = Unpooled.compositeBuffer();
    buf.capacity(original.headerLength());
    original.encode(buf);
    original.release();
    payloadBuf.release();

    // Copy encoded bytes then append 3 extra junk bytes.
    ByteBuf corrupted = Unpooled.buffer(buf.readableBytes() + 3);
    corrupted.writeBytes(buf);
    buf.release();
    corrupted.writeBytes(new byte[]{0x7f, 0x7f, 0x7f});

    try {
      assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessage.decode(corrupted));
    } finally {
      corrupted.release();
    }
  }

  @Test
  public void testDataMessageDecodeRejectsTruncatedFrame() {
    // Encode a valid DataMessage, then strip a trailing byte from the encoded buffer.
    // decode() should reject because dataSize > message.readableBytes() after the header.
    byte[] payload = "hello".getBytes();
    ByteBuf payloadBuf = Unpooled.wrappedBuffer(payload);
    DataMessage original = new DataMessage(1, 2, payload.length, payloadBuf, 0L);
    original.setSeqNum(SEQ_NUM);
    CompositeByteBuf buf = Unpooled.compositeBuffer();
    buf.capacity(original.headerLength());
    original.encode(buf);
    original.release();
    payloadBuf.release();

    int truncatedSize = buf.readableBytes() - 1;
    ByteBuf truncated = Unpooled.buffer(truncatedSize);
    truncated.writeBytes(buf, truncatedSize);
    buf.release();

    try {
      assertThrows(IllegalArgumentException.class,
          () -> StreamingShuffleMessage.decode(truncated));
    } finally {
      truncated.release();
    }
  }

  // ---- ShuffleChecksum -------------------------------------------------------

  @Test
  public void testShuffleChecksumHeapBuffer() {
    byte[] data = {1, 2, 3, 4, 5};
    ByteBuf buf = Unpooled.wrappedBuffer(data); // heap-backed

    ShuffleChecksum cs = new ShuffleChecksum();
    cs.updateChecksum(buf, 0, data.length);
    long value1 = cs.getValue();
    assertTrue(value1 != 0);

    cs.reset();
    cs.updateChecksum(buf, 0, data.length);
    assertEquals(value1, cs.getValue(), "Checksum should be deterministic");
    buf.release();
  }

  @Test
  public void testShuffleChecksumDirectBuffer() {
    byte[] data = {10, 20, 30};
    ByteBuf heap = Unpooled.wrappedBuffer(data);
    ByteBuf direct = Unpooled.directBuffer(data.length);
    direct.writeBytes(data);

    ShuffleChecksum csHeap = new ShuffleChecksum();
    csHeap.updateChecksum(heap, 0, data.length);

    ShuffleChecksum csDirect = new ShuffleChecksum();
    csDirect.updateChecksum(direct, 0, data.length);

    assertEquals(csHeap.getValue(), csDirect.getValue(),
        "Heap and direct buffer checksums should match");

    heap.release();
    direct.release();
  }

  @Test
  public void testShuffleChecksumNonZeroStartIndex() {
    // Verify that startIndex correctly offsets where the checksum begins, and that the
    // result matches an independent reference computation. Cover heap and direct buffers.
    byte[] data = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    int startIndex = 3;
    int dataLength = 5; // bytes 3..7 inclusive

    // Reference CRC32C computed over the same sub-range.
    java.util.zip.CRC32C reference = new java.util.zip.CRC32C();
    reference.update(data, startIndex, dataLength);
    long expected = reference.getValue();

    ByteBuf heap = Unpooled.wrappedBuffer(data);
    ShuffleChecksum csHeap = new ShuffleChecksum();
    csHeap.updateChecksum(heap, startIndex, dataLength);
    assertEquals(expected, csHeap.getValue(),
        "Heap buffer checksum should match reference for sub-range");
    heap.release();

    ByteBuf direct = Unpooled.directBuffer(data.length);
    direct.writeBytes(data);
    ShuffleChecksum csDirect = new ShuffleChecksum();
    csDirect.updateChecksum(direct, startIndex, dataLength);
    assertEquals(expected, csDirect.getValue(),
        "Direct buffer checksum should match reference for sub-range");
    direct.release();
  }

  @Test
  public void testShuffleChecksumRejectsOutOfBoundsRange() {
    // 5 bytes written, but the test asks for bytes [3..8). Should fail because
    // startIndex (3) + dataLength (5) = 8 > writerIndex (5).
    byte[] data = {1, 2, 3, 4, 5};
    ByteBuf buf = Unpooled.wrappedBuffer(data);
    try {
      ShuffleChecksum cs = new ShuffleChecksum();
      assertThrows(IllegalArgumentException.class,
          () -> cs.updateChecksum(buf, 3, 5));
    } finally {
      buf.release();
    }
  }

  @Test
  public void testShuffleChecksumRejectsRangeBeyondWriterIndexButWithinCapacity() {
    // capacity = 10 but only 5 bytes written. Requesting [0..8) should fail because
    // 8 > writerIndex (5), even though 8 <= capacity (10). Guards against checksumming
    // uninitialized bytes.
    ByteBuf buf = Unpooled.buffer(10);
    buf.writeBytes(new byte[]{1, 2, 3, 4, 5});
    try {
      ShuffleChecksum cs = new ShuffleChecksum();
      assertThrows(IllegalArgumentException.class,
          () -> cs.updateChecksum(buf, 0, 8));
    } finally {
      buf.release();
    }
  }

  @Test
  public void testShuffleChecksumRejectsNegativeStartIndex() {
    ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
    try {
      ShuffleChecksum cs = new ShuffleChecksum();
      assertThrows(IllegalArgumentException.class,
          () -> cs.updateChecksum(buf, -1, 2));
    } finally {
      buf.release();
    }
  }

  @Test
  public void testShuffleChecksumRejectsNegativeDataLength() {
    ByteBuf buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
    try {
      ShuffleChecksum cs = new ShuffleChecksum();
      assertThrows(IllegalArgumentException.class,
          () -> cs.updateChecksum(buf, 0, -1));
    } finally {
      buf.release();
    }
  }

  @Test
  public void testInvalidMessageTypeThrows() {
    ByteBuf buf = Unpooled.buffer(12);
    buf.writeInt(999); // unknown type
    buf.writeLong(0L);
    assertThrows(IllegalArgumentException.class, () -> StreamingShuffleMessage.decode(buf));
    buf.release();
  }
}
