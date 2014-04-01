/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Various utility functions for Hadooop record I/O runtime.
 */
public class Utils {
  
  /** Cannot create a new instance of Utils */
  private Utils() {
  }
  
  public static final char[] hexchars = { '0', '1', '2', '3', '4', '5',
                                          '6', '7', '8', '9', 'A', 'B',
                                          'C', 'D', 'E', 'F' };
  /**
   *
   * @param s
   * @return
   */
  static String toXMLString(String s) {
    StringBuffer sb = new StringBuffer();
    for (int idx = 0; idx < s.length(); idx++) {
      char ch = s.charAt(idx);
      if (ch == '<') {
        sb.append("&lt;");
      } else if (ch == '&') {
        sb.append("&amp;");
      } else if (ch == '%') {
        sb.append("%0025");
      } else if (ch < 0x20 ||
                 (ch > 0xD7FF && ch < 0xE000) ||
                 (ch > 0xFFFD)) {
        sb.append("%");
        sb.append(hexchars[(ch & 0xF000) >> 12]);
        sb.append(hexchars[(ch & 0x0F00) >> 8]);
        sb.append(hexchars[(ch & 0x00F0) >> 4]);
        sb.append(hexchars[(ch & 0x000F)]);
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }
  
  static private int h2c(char ch) {
    if (ch >= '0' && ch <= '9') {
      return ch - '0';
    } else if (ch >= 'A' && ch <= 'F') {
      return ch - 'A' + 10;
    } else if (ch >= 'a' && ch <= 'f') {
      return ch - 'a' + 10;
    }
    return 0;
  }
  
  /**
   *
   * @param s
   * @return
   */
  static String fromXMLString(String s) {
    StringBuffer sb = new StringBuffer();
    for (int idx = 0; idx < s.length();) {
      char ch = s.charAt(idx++);
      if (ch == '%') {
        int ch1 = h2c(s.charAt(idx++)) << 12;
        int ch2 = h2c(s.charAt(idx++)) << 8;
        int ch3 = h2c(s.charAt(idx++)) << 4;
        int ch4 = h2c(s.charAt(idx++));
        char res = (char)(ch1 | ch2 | ch3 | ch4);
        sb.append(res);
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }
  
  /**
   *
   * @param s
   * @return
   */
  static String toCSVString(String s) {
    StringBuffer sb = new StringBuffer(s.length()+1);
    sb.append('\'');
    int len = s.length();
    for (int i = 0; i < len; i++) {
      char c = s.charAt(i);
      switch(c) {
      case '\0':
        sb.append("%00");
        break;
      case '\n':
        sb.append("%0A");
        break;
      case '\r':
        sb.append("%0D");
        break;
      case ',':
        sb.append("%2C");
        break;
      case '}':
        sb.append("%7D");
        break;
      case '%':
        sb.append("%25");
        break;
      default:
        sb.append(c);
      }
    }
    return sb.toString();
  }
  
  /**
   *
   * @param s
   * @throws java.io.IOException
   * @return
   */
  static String fromCSVString(String s) throws IOException {
    if (s.charAt(0) != '\'') {
      throw new IOException("Error deserializing string.");
    }
    int len = s.length();
    StringBuffer sb = new StringBuffer(len-1);
    for (int i = 1; i < len; i++) {
      char c = s.charAt(i);
      if (c == '%') {
        char ch1 = s.charAt(i+1);
        char ch2 = s.charAt(i+2);
        i += 2;
        if (ch1 == '0' && ch2 == '0') {
          sb.append('\0');
        } else if (ch1 == '0' && ch2 == 'A') {
          sb.append('\n');
        } else if (ch1 == '0' && ch2 == 'D') {
          sb.append('\r');
        } else if (ch1 == '2' && ch2 == 'C') {
          sb.append(',');
        } else if (ch1 == '7' && ch2 == 'D') {
          sb.append('}');
        } else if (ch1 == '2' && ch2 == '5') {
          sb.append('%');
        } else {
          throw new IOException("Error deserializing string.");
        }
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }
  
  /**
   *
   * @param s
   * @return
   */
  static String toXMLBuffer(Buffer s) {
    return s.toString();
  }
  
  /**
   *
   * @param s
   * @throws java.io.IOException
   * @return
   */
  static Buffer fromXMLBuffer(String s)
    throws IOException {
    if (s.length() == 0) { return new Buffer(); }
    int blen = s.length()/2;
    byte[] barr = new byte[blen];
    for (int idx = 0; idx < blen; idx++) {
      char c1 = s.charAt(2*idx);
      char c2 = s.charAt(2*idx+1);
      barr[idx] = (byte)Integer.parseInt(""+c1+c2, 16);
    }
    return new Buffer(barr);
  }
  
  /**
   *
   * @param buf
   * @return
   */
  static String toCSVBuffer(Buffer buf) {
    StringBuffer sb = new StringBuffer("#");
    sb.append(buf.toString());
    return sb.toString();
  }
  
  /**
   * Converts a CSV-serialized representation of buffer to a new
   * Buffer
   * @param s CSV-serialized representation of buffer
   * @throws java.io.IOException
   * @return Deserialized Buffer
   */
  static Buffer fromCSVBuffer(String s)
    throws IOException {
    if (s.charAt(0) != '#') {
      throw new IOException("Error deserializing buffer.");
    }
    if (s.length() == 1) { return new Buffer(); }
    int blen = (s.length()-1)/2;
    byte[] barr = new byte[blen];
    for (int idx = 0; idx < blen; idx++) {
      char c1 = s.charAt(2*idx+1);
      char c2 = s.charAt(2*idx+2);
      barr[idx] = (byte)Integer.parseInt(""+c1+c2, 16);
    }
    return new Buffer(barr);
  }
  
  private static int utf8LenForCodePoint(final int cpt) throws IOException {
    if (cpt >=0 && cpt <= 0x7F) {
      return 1;
    }
    if (cpt >= 0x80 && cpt <= 0x07FF) {
      return 2;
    }
    if ((cpt >= 0x0800 && cpt < 0xD800) ||
        (cpt > 0xDFFF && cpt <= 0xFFFD)) {
      return 3;
    }
    if (cpt >= 0x10000 && cpt <= 0x10FFFF) {
      return 4;
    }
    throw new IOException("Illegal Unicode Codepoint "+
                          Integer.toHexString(cpt)+" in string.");
  }
  
  private static final int B10 =    Integer.parseInt("10000000", 2);
  private static final int B110 =   Integer.parseInt("11000000", 2);
  private static final int B1110 =  Integer.parseInt("11100000", 2);
  private static final int B11110 = Integer.parseInt("11110000", 2);
  private static final int B11 =    Integer.parseInt("11000000", 2);
  private static final int B111 =   Integer.parseInt("11100000", 2);
  private static final int B1111 =  Integer.parseInt("11110000", 2);
  private static final int B11111 = Integer.parseInt("11111000", 2);
  
  private static int writeUtf8(int cpt, final byte[] bytes, final int offset)
    throws IOException {
    if (cpt >=0 && cpt <= 0x7F) {
      bytes[offset] = (byte) cpt;
      return 1;
    }
    if (cpt >= 0x80 && cpt <= 0x07FF) {
      bytes[offset+1] = (byte) (B10 | (cpt & 0x3F));
      cpt = cpt >> 6;
      bytes[offset] = (byte) (B110 | (cpt & 0x1F));
      return 2;
    }
    if ((cpt >= 0x0800 && cpt < 0xD800) ||
        (cpt > 0xDFFF && cpt <= 0xFFFD)) {
      bytes[offset+2] = (byte) (B10 | (cpt & 0x3F));
      cpt = cpt >> 6;
      bytes[offset+1] = (byte) (B10 | (cpt & 0x3F));
      cpt = cpt >> 6;
      bytes[offset] = (byte) (B1110 | (cpt & 0x0F));
      return 3;
    }
    if (cpt >= 0x10000 && cpt <= 0x10FFFF) {
      bytes[offset+3] = (byte) (B10 | (cpt & 0x3F));
      cpt = cpt >> 6;
      bytes[offset+2] = (byte) (B10 | (cpt & 0x3F));
      cpt = cpt >> 6;
      bytes[offset+1] = (byte) (B10 | (cpt & 0x3F));
      cpt = cpt >> 6;
      bytes[offset] = (byte) (B11110 | (cpt & 0x07));
      return 4;
    }
    throw new IOException("Illegal Unicode Codepoint "+
                          Integer.toHexString(cpt)+" in string.");
  }
  
  static void toBinaryString(final DataOutput out, final String str)
    throws IOException {
    final int strlen = str.length();
    byte[] bytes = new byte[strlen*4]; // Codepoints expand to 4 bytes max
    int utf8Len = 0;
    int idx = 0;
    while(idx < strlen) {
      final int cpt = str.codePointAt(idx);
      idx += Character.isSupplementaryCodePoint(cpt) ? 2 : 1;
      utf8Len += writeUtf8(cpt, bytes, utf8Len);
    }
    writeVInt(out, utf8Len);
    out.write(bytes, 0, utf8Len);
  }
  
  static boolean isValidCodePoint(int cpt) {
    return !((cpt > 0x10FFFF) ||
             (cpt >= 0xD800 && cpt <= 0xDFFF) ||
             (cpt >= 0xFFFE && cpt <=0xFFFF));
  }
  
  private static int utf8ToCodePoint(int b1, int b2, int b3, int b4) {
    int cpt = 0;
    cpt = (((b1 & ~B11111) << 18) |
           ((b2 & ~B11) << 12) |
           ((b3 & ~B11) << 6) |
           (b4 & ~B11));
    return cpt;
  }
  
  private static int utf8ToCodePoint(int b1, int b2, int b3) {
    int cpt = 0;
    cpt = (((b1 & ~B1111) << 12) | ((b2 & ~B11) << 6) | (b3 & ~B11));
    return cpt;
  }
  
  private static int utf8ToCodePoint(int b1, int b2) {
    int cpt = 0;
    cpt = (((b1 & ~B111) << 6) | (b2 & ~B11));
    return cpt;
  }
  
  private static void checkB10(int b) throws IOException {
    if ((b & B11) != B10) {
      throw new IOException("Invalid UTF-8 representation.");
    }
  }
  
  static String fromBinaryString(final DataInput din) throws IOException {
    final int utf8Len = readVInt(din);
    final byte[] bytes = new byte[utf8Len];
    din.readFully(bytes);
    int len = 0;
    // For the most commmon case, i.e. ascii, numChars = utf8Len
    StringBuilder sb = new StringBuilder(utf8Len);
    while(len < utf8Len) {
      int cpt = 0;
      final int b1 = bytes[len++] & 0xFF;
      if (b1 <= 0x7F) {
        cpt = b1;
      } else if ((b1 & B11111) == B11110) {
        int b2 = bytes[len++] & 0xFF;
        checkB10(b2);
        int b3 = bytes[len++] & 0xFF;
        checkB10(b3);
        int b4 = bytes[len++] & 0xFF;
        checkB10(b4);
        cpt = utf8ToCodePoint(b1, b2, b3, b4);
      } else if ((b1 & B1111) == B1110) {
        int b2 = bytes[len++] & 0xFF;
        checkB10(b2);
        int b3 = bytes[len++] & 0xFF;
        checkB10(b3);
        cpt = utf8ToCodePoint(b1, b2, b3);
      } else if ((b1 & B111) == B110) {
        int b2 = bytes[len++] & 0xFF;
        checkB10(b2);
        cpt = utf8ToCodePoint(b1, b2);
      } else {
        throw new IOException("Invalid UTF-8 byte "+Integer.toHexString(b1)+
                              " at offset "+(len-1)+" in length of "+utf8Len);
      }
      if (!isValidCodePoint(cpt)) {
        throw new IOException("Illegal Unicode Codepoint "+
                              Integer.toHexString(cpt)+" in stream.");
      }
      sb.appendCodePoint(cpt);
    }
    return sb.toString();
  }
  
  /** Parse a float from a byte array. */
  public static float readFloat(byte[] bytes, int start) {
    return WritableComparator.readFloat(bytes, start);
  }
  
  /** Parse a double from a byte array. */
  public static double readDouble(byte[] bytes, int start) {
    return WritableComparator.readDouble(bytes, start);
  }
  
  /**
   * Reads a zero-compressed encoded long from a byte array and returns it.
   * @param bytes byte array with decode long
   * @param start starting index
   * @throws java.io.IOException
   * @return deserialized long
   */
  public static long readVLong(byte[] bytes, int start) throws IOException {
    return WritableComparator.readVLong(bytes, start);
  }
  
  /**
   * Reads a zero-compressed encoded integer from a byte array and returns it.
   * @param bytes byte array with the encoded integer
   * @param start start index
   * @throws java.io.IOException
   * @return deserialized integer
   */
  public static int readVInt(byte[] bytes, int start) throws IOException {
    return WritableComparator.readVInt(bytes, start);
  }
  
  /**
   * Reads a zero-compressed encoded long from a stream and return it.
   * @param in input stream
   * @throws java.io.IOException
   * @return deserialized long
   */
  public static long readVLong(DataInput in) throws IOException {
    return WritableUtils.readVLong(in);
  }
  
  /**
   * Reads a zero-compressed encoded integer from a stream and returns it.
   * @param in input stream
   * @throws java.io.IOException
   * @return deserialized integer
   */
  public static int readVInt(DataInput in) throws IOException {
    return WritableUtils.readVInt(in);
  }
  
  /**
   * Get the encoded length if an integer is stored in a variable-length format
   * @return the encoded length
   */
  public static int getVIntSize(long i) {
    return WritableUtils.getVIntSize(i);
  }
  
  /**
   * Serializes a long to a binary stream with zero-compressed encoding.
   * For -112 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * long is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -113 and -120, the following long
   * is positive, with number of bytes that follow are -(v+112).
   * If the first byte value v is between -121 and -128, the following long
   * is negative, with number of bytes that follow are -(v+120). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param stream Binary output stream
   * @param i Long to be serialized
   * @throws java.io.IOException
   */
  public static void writeVLong(DataOutput stream, long i) throws IOException {
    WritableUtils.writeVLong(stream, i);
  }
  
  /**
   * Serializes an int to a binary stream with zero-compressed encoding.
   *
   * @param stream Binary output stream
   * @param i int to be serialized
   * @throws java.io.IOException
   */
  public static void writeVInt(DataOutput stream, int i) throws IOException {
    WritableUtils.writeVInt(stream, i);
  }
  
  /** Lexicographic order of binary data. */
  public static int compareBytes(byte[] b1, int s1, int l1,
                                 byte[] b2, int s2, int l2) {
    return WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2);
  }
}
