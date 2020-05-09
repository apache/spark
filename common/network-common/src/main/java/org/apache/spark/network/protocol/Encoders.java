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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import org.roaringbitmap.RoaringBitmap;

/** Provides a canonical set of Encoders for simple types. */
public class Encoders {

  /** Strings are encoded with their length followed by UTF-8 bytes. */
  public static class Strings {
    public static int encodedLength(String s) {
      return 4 + s.getBytes(StandardCharsets.UTF_8).length;
    }

    public static void encode(ByteBuf buf, String s) {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      buf.writeInt(bytes.length);
      buf.writeBytes(bytes);
    }

    public static String decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  /** Bitmaps are encoded with their serialization length followed by the serialization bytes. */
  public static class Bitmaps {
    public static int encodedLength(RoaringBitmap b) {
      return 4 + b.serializedSizeInBytes();
    }

    public static void encode(ByteBuf buf, RoaringBitmap b) {
      // Compress the bitmap before serializing it
      b.trim();
      b.runOptimize();
      ByteBuffer outBuffer = ByteBuffer.allocate(b.serializedSizeInBytes());
      try {
        b.serialize(new DataOutputStream(new OutputStream() {
          ByteBuffer buffer;

          OutputStream init(ByteBuffer buffer) {
            this.buffer = buffer;
            return this;
          }

          @Override
          public void close() {
          }

          @Override
          public void flush() {
          }

          @Override
          public void write(int b) {
            buffer.put((byte) b);
          }

          @Override
          public void write(byte[] b) {
            buffer.put(b);
          }

          @Override
          public void write(byte[] b, int off, int l) {
            buffer.put(b, off, l);
          }
        }.init(outBuffer)));
      } catch (IOException e) {
        throw new RuntimeException("Exception while encoding bitmap", e);
      }
      byte[] bytes = outBuffer.array();
      buf.writeInt(bytes.length);
      buf.writeBytes(bytes);
    }

    public static RoaringBitmap decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      RoaringBitmap bitmap = new RoaringBitmap();
      try {
        bitmap.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
      } catch (IOException e) {
        throw new RuntimeException("Exception while decoding bitmap", e);
      }
      return bitmap;
    }
  }

  /** Byte arrays are encoded with their length followed by bytes. */
  public static class ByteArrays {
    public static int encodedLength(byte[] arr) {
      return 4 + arr.length;
    }

    public static void encode(ByteBuf buf, byte[] arr) {
      buf.writeInt(arr.length);
      buf.writeBytes(arr);
    }

    public static byte[] decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return bytes;
    }
  }

  /** String arrays are encoded with the number of strings followed by per-String encoding. */
  public static class StringArrays {
    public static int encodedLength(String[] strings) {
      int totalLength = 4;
      for (String s : strings) {
        totalLength += Strings.encodedLength(s);
      }
      return totalLength;
    }

    public static void encode(ByteBuf buf, String[] strings) {
      buf.writeInt(strings.length);
      for (String s : strings) {
        Strings.encode(buf, s);
      }
    }

    public static String[] decode(ByteBuf buf) {
      int numStrings = buf.readInt();
      String[] strings = new String[numStrings];
      for (int i = 0; i < strings.length; i ++) {
        strings[i] = Strings.decode(buf);
      }
      return strings;
    }
  }

  /** Integer arrays are encoded with their length followed by integers. */
  public static class IntArrays {
    public static int encodedLength(int[] ints) {
      return 4 + 4 * ints.length;
    }

    public static void encode(ByteBuf buf, int[] ints) {
      buf.writeInt(ints.length);
      for (int i : ints) {
        buf.writeInt(i);
      }
    }

    public static int[] decode(ByteBuf buf) {
      int numInts = buf.readInt();
      int[] ints = new int[numInts];
      for (int i = 0; i < ints.length; i ++) {
        ints[i] = buf.readInt();
      }
      return ints;
    }
  }

  /** Long integer arrays are encoded with their length followed by long integers. */
  public static class LongArrays {
    public static int encodedLength(long[] longs) {
      return 4 + 8 * longs.length;
    }

    public static void encode(ByteBuf buf, long[] longs) {
      buf.writeInt(longs.length);
      for (long i : longs) {
        buf.writeLong(i);
      }
    }

    public static long[] decode(ByteBuf buf) {
      int numLongs = buf.readInt();
      long[] longs = new long[numLongs];
      for (int i = 0; i < longs.length; i ++) {
        longs[i] = buf.readLong();
      }
      return longs;
    }
  }

  /** Bitmap arrays are encoded with the number of bitmaps followed by per-Bitmap encoding. */
  public static class BitmapArrays {
    public static int encodedLength(RoaringBitmap[] bitmaps) {
      int totalLength = 4;
      for (RoaringBitmap b : bitmaps) {
        totalLength += Bitmaps.encodedLength(b);
      }
      return totalLength;
    }

    public static void encode(ByteBuf buf, RoaringBitmap[] bitmaps) {
      buf.writeInt(bitmaps.length);
      for (RoaringBitmap b : bitmaps) {
        Bitmaps.encode(buf, b);
      }
    }

    public static RoaringBitmap[] decode(ByteBuf buf) {
      int numBitmaps = buf.readInt();
      RoaringBitmap[] bitmaps = new RoaringBitmap[numBitmaps];
      for (int i = 0; i < bitmaps.length; i ++) {
        bitmaps[i] = Bitmaps.decode(buf);
      }
      return bitmaps;
    }
  }
}
