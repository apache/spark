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

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import com.google.common.io.ByteStreams;

/** Provides a canonical set of Encoders for simple types. */
public class Encoders {

  public static class Longs {
    public static int encodedLength(long l) {
      return 8;
    }

    public static void encode(OutputStream out, long l) throws IOException {
      byte[] bytes = com.google.common.primitives.Longs.toByteArray(l);
      out.write(bytes);
    }

    public static long decode(InputStream in) throws IOException {
      byte[] bytes = new byte[8];
      ByteStreams.readFully(in, bytes);
      return com.google.common.primitives.Longs.fromByteArray(bytes);
    }

  }

  public static class Doubles {
    public static int encodedLength(long l) {
      return 8;
    }

    public static void encode(OutputStream out, double d) throws IOException {
      long l = java.lang.Double.doubleToLongBits(d);
      Longs.encode(out, l);
    }

    public static double decode(InputStream in) throws IOException {
      byte[] bytes = new byte[8];
      ByteStreams.readFully(in, bytes);
      long l = com.google.common.primitives.Longs.fromByteArray(bytes);
      return Double.longBitsToDouble(l);
    }
  }

  public static class Ints {
    public static int encodedLength(int i) {
      return 4;
    }

    public static void encode(OutputStream out, int i) throws IOException {
      byte[] bytes = com.google.common.primitives.Ints.toByteArray(i);
      out.write(bytes);
    }

    public static int decode(InputStream in) throws IOException {
      byte[] bytes = new byte[4];
      ByteStreams.readFully(in, bytes);
      return com.google.common.primitives.Ints.fromByteArray(bytes);
    }
  }

  /**
   * Strings are encoded with their length followed by UTF-8 bytes.
   */
  public static class Strings {
    public static int encodedLength(String s) {
      return 4 + s.getBytes(StandardCharsets.UTF_8).length;
    }

    public static void encode(OutputStream out, String s) throws IOException {
      byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
      Ints.encode(out, bytes.length);
      out.write(bytes);
    }

    public static String decode(InputStream in) throws IOException {
      int length = Ints.decode(in);
      byte[] bytes = new byte[length];
      ByteStreams.readFully(in, bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  /**
   * Byte is encoded with their length followed by bytes.
   */
  public static class Bytes {
    public static int encodedLength(byte arr) {
      return 1;
    }

    public static void encode(OutputStream out, byte arr) throws IOException {
      out.write(arr);
    }

    public static byte decode(InputStream in) throws IOException {
      int ch = in.read();
      if (ch < 0)
        throw new EOFException();
      return (byte) (ch);
    }
  }

  /**
   * Byte arrays are encoded with their length followed by bytes.
   */
  public static class ByteArrays {
    public static int encodedLength(byte[] arr) {
      return 4 + arr.length;
    }

    public static void encode(OutputStream out, byte[] arr) throws IOException {
      Ints.encode(out, arr.length);
      out.write(arr);
    }

    public static byte[] decode(InputStream in) throws IOException {
      int length = Ints.decode(in);
      byte[] bytes = new byte[length];
      ByteStreams.readFully(in, bytes);
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

    public static void encode(OutputStream out, String[] strings) throws IOException {
      Ints.encode(out, strings.length);
      for (String s : strings) {
        Strings.encode(out, s);
      }
    }

    public static String[] decode(InputStream in) throws IOException {
      int numStrings = Ints.decode(in);
      String[] strings = new String[numStrings];
      for (int i = 0; i < strings.length; i++) {
        strings[i] = Strings.decode(in);
      }
      return strings;
    }
  }
}
