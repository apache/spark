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

package org.apache.spark.unsafe.types;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryAllocator;
import org.apache.spark.unsafe.memory.MemoryBlock;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BinaryViewSuite {

  private static final byte[] DATA = new byte[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };

  @Test
  public void nullFactoriesReturnNull() {
    assertNull(BinaryView.fromBytes(null));
    assertNull(BinaryView.fromBytes(null, 0, 0));
  }

  @Test
  public void onHeapFromBytesReferencesBackingArray() {
    BinaryView v = BinaryView.fromBytes(DATA);
    assertEquals(DATA.length, v.numBytes());
    assertFalse(v.isOffHeap());
    assertSame(DATA, v.getBaseObject());
    // getBytes() must return the backing array when the view owns a tight, on-heap byte[].
    assertSame(DATA, v.getBytes());
  }

  @Test
  public void sliceOfHeapBytes() {
    BinaryView full = BinaryView.fromBytes(DATA);
    BinaryView mid = full.slice(2, 4);
    assertEquals(4, mid.numBytes());
    // Slice shares the backing array but is not a tight owner, so getBytes() must copy.
    assertArrayEquals(new byte[] { 30, 40, 50, 60 }, mid.getBytes());
    assertNotSame(DATA, mid.getBytes());
    // Range reads use slice-relative coordinates.
    assertEquals(30, mid.getByte(0));
    assertEquals(60, mid.getByte(3));
  }

  @Test
  public void hasTightOnHeapArray() {
    // A view that owns the whole array is a tight on-heap owner.
    assertTrue(BinaryView.fromBytes(DATA).hasTightOnHeapArray());
    // A sub-range view is not, even when on-heap.
    assertFalse(BinaryView.fromBytes(DATA, 2, 4).hasTightOnHeapArray());
    assertFalse(BinaryView.fromBytes(DATA).slice(0, DATA.length - 1).hasTightOnHeapArray());
    // copy() always produces a tight on-heap owner.
    assertTrue(BinaryView.fromBytes(DATA, 2, 4).copy().hasTightOnHeapArray());

    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(DATA.length);
    try {
      Platform.copyMemory(DATA, Platform.BYTE_ARRAY_OFFSET,
        null, block.getBaseOffset(), DATA.length);
      BinaryView offHeap = BinaryView.fromAddress(null, block.getBaseOffset(), DATA.length);
      // An off-heap view is never a tight on-heap owner.
      assertFalse(offHeap.hasTightOnHeapArray());
      assertTrue(offHeap.copy().hasTightOnHeapArray());
    } finally {
      MemoryAllocator.UNSAFE.free(block);
    }
  }

  @Test
  public void primitiveReaders() {
    byte[] bytes = new byte[16];
    Platform.putInt(bytes, Platform.BYTE_ARRAY_OFFSET, 0xCAFEBABE);
    Platform.putLong(bytes, Platform.BYTE_ARRAY_OFFSET + 8, 0x1234567890ABCDEFL);
    BinaryView v = BinaryView.fromBytes(bytes);
    assertEquals(0xCAFEBABE, v.getInt(0));
    assertEquals(0x1234567890ABCDEFL, v.getLong(8));
  }

  @Test
  public void copyIsIndependent() {
    byte[] bytes = DATA.clone();
    BinaryView v = BinaryView.fromBytes(bytes);
    BinaryView c = v.copy();
    assertNotSame(v.getBaseObject(), c.getBaseObject());
    assertArrayEquals(DATA, c.getBytes());
    // Mutating the source must not affect the copy.
    bytes[0] = 99;
    assertEquals(10, c.getBytes()[0]);
  }

  @Test
  public void offHeapView() {
    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(DATA.length);
    try {
      Platform.copyMemory(DATA, Platform.BYTE_ARRAY_OFFSET,
        null, block.getBaseOffset(), DATA.length);
      BinaryView v = BinaryView.fromAddress(null, block.getBaseOffset(), DATA.length);
      assertTrue(v.isOffHeap());
      assertNull(v.getBaseObject());
      assertEquals(DATA.length, v.numBytes());
      // getBytes() on an off-heap view must materialize a new array.
      byte[] materialized = v.getBytes();
      assertArrayEquals(DATA, materialized);
      // copy() materializes to an on-heap, tight owner.
      BinaryView c = v.copy();
      assertFalse(c.isOffHeap());
      assertArrayEquals(DATA, c.getBytes());
    } finally {
      MemoryAllocator.UNSAFE.free(block);
    }
  }

  @Test
  public void equalsAcrossHeapAndOffHeap() {
    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(DATA.length);
    try {
      Platform.copyMemory(DATA, Platform.BYTE_ARRAY_OFFSET,
        null, block.getBaseOffset(), DATA.length);
      BinaryView heap = BinaryView.fromBytes(DATA);
      BinaryView offHeap = BinaryView.fromAddress(null, block.getBaseOffset(), DATA.length);
      assertEquals(heap, offHeap);
      assertEquals(heap.hashCode(), offHeap.hashCode());
    } finally {
      MemoryAllocator.UNSAFE.free(block);
    }
  }

  @Test
  public void compareToIsUnsignedLexicographic() {
    BinaryView a = BinaryView.fromBytes(new byte[] { 1, 2, 3 });
    BinaryView b = BinaryView.fromBytes(new byte[] { 1, 2, 4 });
    BinaryView c = BinaryView.fromBytes(new byte[] { 1, 2, 3, 0 });
    BinaryView neg = BinaryView.fromBytes(new byte[] { (byte) 0x80 });
    BinaryView pos = BinaryView.fromBytes(new byte[] { 0x7F });
    assertTrue(a.compareTo(b) < 0);
    assertTrue(b.compareTo(a) > 0);
    assertTrue(a.compareTo(c) < 0); // shorter prefix is less
    assertEquals(0, a.compareTo(BinaryView.fromBytes(new byte[] { 1, 2, 3 })));
    // Unsigned byte comparison: 0x80 > 0x7F.
    assertTrue(neg.compareTo(pos) > 0);
  }

  @Test
  public void byteBufferRoundTripHeap() {
    BinaryView v = BinaryView.fromBytes(DATA);
    ByteBuffer bb = v.toByteBuffer();
    assertTrue(bb.hasArray());
    assertEquals(DATA.length, bb.remaining());
    byte[] out = new byte[DATA.length];
    bb.get(out);
    assertArrayEquals(DATA, out);
  }

  @Test
  public void byteBufferOffHeapMaterializes() {
    MemoryBlock block = MemoryAllocator.UNSAFE.allocate(DATA.length);
    try {
      Platform.copyMemory(DATA, Platform.BYTE_ARRAY_OFFSET,
        null, block.getBaseOffset(), DATA.length);
      BinaryView v = BinaryView.fromAddress(null, block.getBaseOffset(), DATA.length);
      ByteBuffer bb = v.toByteBuffer();
      // For off-heap, toByteBuffer materializes into a fresh on-heap array.
      assertTrue(bb.hasArray());
      byte[] out = new byte[DATA.length];
      bb.get(out);
      assertArrayEquals(DATA, out);
    } finally {
      MemoryAllocator.UNSAFE.free(block);
    }
  }

  @Test
  public void writeToMemoryRoundTrip() {
    BinaryView v = BinaryView.fromBytes(DATA);
    byte[] target = new byte[DATA.length + 4];
    v.writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + 2);
    for (int i = 0; i < DATA.length; i++) {
      assertEquals(DATA[i], target[i + 2]);
    }
  }

  @Test
  public void javaSerializationRoundTrip() throws Exception {
    // Serialize a view that points at a sub-range of a larger array; deserialized value
    // must own a tight backing array containing only the visible bytes.
    BinaryView v = BinaryView.fromBytes(DATA, 2, 4);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
      out.writeObject(v);
    }
    BinaryView read;
    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
      read = (BinaryView) in.readObject();
    }
    assertEquals(4, read.numBytes());
    assertArrayEquals(new byte[] { 30, 40, 50, 60 }, read.getBytes());
    assertFalse(read.isOffHeap());
    assertEquals(v, read);
  }

  @Test
  public void kryoSerializationRoundTrip() {
    Kryo kryo = new Kryo();
    kryo.register(BinaryView.class);
    BinaryView v = BinaryView.fromBytes(DATA, 3, 5);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (Output out = new Output(baos)) {
      kryo.writeObject(out, v);
    }
    BinaryView read;
    try (Input in = new Input(new ByteArrayInputStream(baos.toByteArray()))) {
      read = kryo.readObject(in, BinaryView.class);
    }
    assertEquals(5, read.numBytes());
    assertArrayEquals(new byte[] { 40, 50, 60, 70, 80 }, read.getBytes());
    assertEquals(v, read);
  }

}
