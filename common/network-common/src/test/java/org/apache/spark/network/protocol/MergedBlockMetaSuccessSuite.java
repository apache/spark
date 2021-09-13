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

package org.apache.spark.network.protocol;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.util.List;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.mockito.Mockito.*;

import org.apache.spark.network.buffer.FileSegmentManagedBuffer;
import org.apache.spark.network.util.ByteArrayWritableChannel;
import org.apache.spark.network.util.TransportConf;

/**
 * Test for {@link MergedBlockMetaSuccess}.
 */
public class MergedBlockMetaSuccessSuite {

  @Test
  public void testMergedBlocksMetaEncodeDecode() throws Exception {
    File chunkMetaFile = new File("target/mergedBlockMetaTest");
    Files.deleteIfExists(chunkMetaFile.toPath());
    RoaringBitmap chunk1 = new RoaringBitmap();
    chunk1.add(1);
    chunk1.add(3);
    RoaringBitmap chunk2 = new RoaringBitmap();
    chunk2.add(2);
    chunk2.add(4);
    RoaringBitmap[] expectedChunks = new RoaringBitmap[]{chunk1, chunk2};
    try (DataOutputStream metaOutput = new DataOutputStream(new FileOutputStream(chunkMetaFile))) {
      for (RoaringBitmap expectedChunk : expectedChunks) {
        expectedChunk.serialize(metaOutput);
      }
    }
    TransportConf conf = mock(TransportConf.class);
    when(conf.lazyFileDescriptor()).thenReturn(false);
    long requestId = 1L;
    MergedBlockMetaSuccess expectedMeta = new MergedBlockMetaSuccess(requestId, 2,
      new FileSegmentManagedBuffer(conf, chunkMetaFile, 0, chunkMetaFile.length()));

    List<Object> out = Lists.newArrayList();
    ChannelHandlerContext context = mock(ChannelHandlerContext.class);
    when(context.alloc()).thenReturn(ByteBufAllocator.DEFAULT);

    MessageEncoder.INSTANCE.encode(context, expectedMeta, out);
    Assert.assertEquals(1, out.size());
    MessageWithHeader msgWithHeader = (MessageWithHeader) out.remove(0);

    ByteArrayWritableChannel writableChannel =
      new ByteArrayWritableChannel((int) msgWithHeader.count());
    while (msgWithHeader.transfered() < msgWithHeader.count()) {
      msgWithHeader.transferTo(writableChannel, msgWithHeader.transfered());
    }
    ByteBuf messageBuf = Unpooled.wrappedBuffer(writableChannel.getData());
    messageBuf.readLong(); // frame length
    MessageDecoder.INSTANCE.decode(mock(ChannelHandlerContext.class), messageBuf, out);
    Assert.assertEquals(1, out.size());
    MergedBlockMetaSuccess decoded = (MergedBlockMetaSuccess) out.get(0);
    Assert.assertEquals("merged block", expectedMeta.requestId, decoded.requestId);
    Assert.assertEquals("num chunks", expectedMeta.getNumChunks(), decoded.getNumChunks());

    ByteBuf responseBuf = Unpooled.wrappedBuffer(decoded.body().nioByteBuffer());
    RoaringBitmap[] responseBitmaps = new RoaringBitmap[expectedMeta.getNumChunks()];
    for (int i = 0; i < expectedMeta.getNumChunks(); i++) {
      responseBitmaps[i] = Encoders.Bitmaps.decode(responseBuf);
    }
    Assert.assertEquals(
      "num of roaring bitmaps", expectedMeta.getNumChunks(), responseBitmaps.length);
    for (int i = 0; i < expectedMeta.getNumChunks(); i++) {
      Assert.assertEquals("chunk bitmap " + i, expectedChunks[i], responseBitmaps[i]);
    }
    Files.delete(chunkMetaFile.toPath());
  }
}
