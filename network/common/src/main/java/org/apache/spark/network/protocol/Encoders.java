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


import com.google.common.base.Charsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/** Provides a canonical set of Encoders for simple types. */
public class Encoders {

  /** Strings are encoded with their length followed by UTF-8 bytes. */
  public static class Strings {
    public static int encodedLength(String s) {
      return 4 + s.getBytes(Charsets.UTF_8).length;
    }

    public static void encode(ByteBuf buf, String s) {
      byte[] bytes = s.getBytes(Charsets.UTF_8);
      buf.writeInt(bytes.length);
      buf.writeBytes(bytes);
    }

    public static String decode(ByteBuf buf) {
      int length = buf.readInt();
      byte[] bytes = new byte[length];
      buf.readBytes(bytes);
      return new String(bytes, Charsets.UTF_8);
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
}
