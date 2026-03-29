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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link Encoders}.
 */
public class EncodersSuite {

  @Test
  public void testRoaringBitmapEncodeDecode() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(1, 2, 3);
    ByteBuf buf = Unpooled.buffer(Encoders.Bitmaps.encodedLength(bitmap));
    Encoders.Bitmaps.encode(buf, bitmap);
    RoaringBitmap decodedBitmap = Encoders.Bitmaps.decode(buf);
    assertEquals(bitmap, decodedBitmap);
  }

  @Test
  public void testRoaringBitmapEncodeShouldFailWhenBufferIsSmall() {
    RoaringBitmap bitmap = new RoaringBitmap();
    bitmap.add(1, 2, 3);
    ByteBuf buf = Unpooled.buffer(4);
    assertThrows(java.nio.BufferOverflowException.class,
      () -> Encoders.Bitmaps.encode(buf, bitmap));
  }

  @Test
  public void testBitmapArraysEncodeDecode() {
    RoaringBitmap[] bitmaps = new RoaringBitmap[] {
      new RoaringBitmap(),
      new RoaringBitmap(),
      new RoaringBitmap(), // empty
      new RoaringBitmap(),
      new RoaringBitmap()
    };
    bitmaps[0].add(1, 2, 3);
    bitmaps[1].add(1, 2, 4);
    bitmaps[3].add(7L, 9L);
    bitmaps[4].add(1L, 100L);
    ByteBuf buf = Unpooled.buffer(Encoders.BitmapArrays.encodedLength(bitmaps));
    Encoders.BitmapArrays.encode(buf, bitmaps);
    RoaringBitmap[] decodedBitmaps = Encoders.BitmapArrays.decode(buf);
    assertArrayEquals(bitmaps, decodedBitmaps);
  }
}
