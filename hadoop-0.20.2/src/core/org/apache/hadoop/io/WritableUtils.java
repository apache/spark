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

package org.apache.hadoop.io;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class WritableUtils  {

  public static byte[] readCompressedByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    if (length == -1) return null;
    byte[] buffer = new byte[length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    GZIPInputStream gzi = new GZIPInputStream(new ByteArrayInputStream(buffer, 0, buffer.length));
    byte[] outbuf = new byte[length];
    ByteArrayOutputStream bos =  new ByteArrayOutputStream();
    int len;
    while((len=gzi.read(outbuf, 0, outbuf.length)) != -1){
      bos.write(outbuf, 0, len);
    }
    byte[] decompressed =  bos.toByteArray();
    bos.close();
    gzi.close();
    return decompressed;
  }

  public static void skipCompressedByteArray(DataInput in) throws IOException {
    int length = in.readInt();
    if (length != -1) {
      skipFully(in, length);
    }
  }

  public static int  writeCompressedByteArray(DataOutput out, 
                                              byte[] bytes) throws IOException {
    if (bytes != null) {
      ByteArrayOutputStream bos =  new ByteArrayOutputStream();
      GZIPOutputStream gzout = new GZIPOutputStream(bos);
      gzout.write(bytes, 0, bytes.length);
      gzout.close();
      byte[] buffer = bos.toByteArray();
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
      /* debug only! Once we have confidence, can lose this. */
      return ((bytes.length != 0) ? (100*buffer.length)/bytes.length : 0);
    } else {
      out.writeInt(-1);
      return -1;
    }
  }


  /* Ugly utility, maybe someone else can do this better  */
  public static String readCompressedString(DataInput in) throws IOException {
    byte[] bytes = readCompressedByteArray(in);
    if (bytes == null) return null;
    return new String(bytes, "UTF-8");
  }


  public static int  writeCompressedString(DataOutput out, String s) throws IOException {
    return writeCompressedByteArray(out, (s != null) ? s.getBytes("UTF-8") : null);
  }

  /*
   *
   * Write a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   * 
   */
  public static void writeString(DataOutput out, String s) throws IOException {
    if (s != null) {
      byte[] buffer = s.getBytes("UTF-8");
      int len = buffer.length;
      out.writeInt(len);
      out.write(buffer, 0, len);
    } else {
      out.writeInt(-1);
    }
  }

  /*
   * Read a String as a Network Int n, followed by n Bytes
   * Alternative to 16 bit read/writeUTF.
   * Encoding standard is... ?
   *
   */
  public static String readString(DataInput in) throws IOException{
    int length = in.readInt();
    if (length == -1) return null;
    byte[] buffer = new byte[length];
    in.readFully(buffer);      // could/should use readFully(buffer,0,length)?
    return new String(buffer,"UTF-8");  
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection.
   *
   */
  public static void writeStringArray(DataOutput out, String[] s) throws IOException{
    out.writeInt(s.length);
    for(int i = 0; i < s.length; i++) {
      writeString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array of
   * compressed Strings. Handles also null arrays and null values.
   * Could be generalised using introspection.
   *
   */
  public static void writeCompressedStringArray(DataOutput out, String[] s) throws IOException{
    if (s == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(s.length);
    for(int i = 0; i < s.length; i++) {
      writeCompressedString(out, s[i]);
    }
  }

  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Actually this bit couldn't...
   *
   */
  public static String[] readStringArray(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == -1) return null;
    String[] s = new String[len];
    for(int i = 0; i < len; i++) {
      s[i] = readString(in);
    }
    return s;
  }


  /*
   * Write a String array as a Nework Int N, followed by Int N Byte Array Strings.
   * Could be generalised using introspection. Handles null arrays and null values.
   *
   */
  public static  String[] readCompressedStringArray(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == -1) return null;
    String[] s = new String[len];
    for(int i = 0; i < len; i++) {
      s[i] = readCompressedString(in);
    }
    return s;
  }


  /*
   *
   * Test Utility Method Display Byte Array. 
   *
   */
  public static void displayByteArray(byte[] record){
    int i;
    for(i=0;i < record.length -1; i++){
      if (i % 16 == 0) { System.out.println(); }
      System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
      System.out.print(Integer.toHexString(record[i] & 0x0F));
      System.out.print(",");
    }
    System.out.print(Integer.toHexString(record[i]  >> 4 & 0x0F));
    System.out.print(Integer.toHexString(record[i] & 0x0F));
    System.out.println();
  }

  /**
   * Make a copy of a writable object using serialization to a buffer.
   * @param orig The object to copy
   * @return The copied object
   */
  public static <T extends Writable> T clone(T orig, Configuration conf) {
    try {
      @SuppressWarnings("unchecked") // Unchecked cast from Class to Class<T>
      T newInst = ReflectionUtils.newInstance((Class<T>) orig.getClass(), conf);
      ReflectionUtils.copy(conf, orig, newInst);
      return newInst;
    } catch (IOException e) {
      throw new RuntimeException("Error writing/reading clone buffer", e);
    }
  }

  /**
   * Make a copy of the writable object using serialiation to a buffer
   * @param dst the object to copy from
   * @param src the object to copy into, which is destroyed
   * @throws IOException
   * @deprecated use ReflectionUtils.cloneInto instead.
   */
  @Deprecated
  public static void cloneInto(Writable dst, Writable src) throws IOException {
    ReflectionUtils.cloneWritableInto(dst, src);
  }

  /**
   * Serializes an integer to a binary stream with zero-compressed encoding.
   * For -120 <= i <= 127, only one byte is used with the actual value.
   * For other values of i, the first byte value indicates whether the
   * integer is positive or negative, and the number of bytes that follow.
   * If the first byte value v is between -121 and -124, the following integer
   * is positive, with number of bytes that follow are -(v+120).
   * If the first byte value v is between -125 and -128, the following integer
   * is negative, with number of bytes that follow are -(v+124). Bytes are
   * stored in the high-non-zero-byte-first order.
   *
   * @param stream Binary output stream
   * @param i Integer to be serialized
   * @throws java.io.IOException 
   */
  public static void writeVInt(DataOutput stream, int i) throws IOException {
    writeVLong(stream, i);
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
    if (i >= -112 && i <= 127) {
      stream.writeByte((byte)i);
      return;
    }
      
    int len = -112;
    if (i < 0) {
      i ^= -1L; // take one's complement'
      len = -120;
    }
      
    long tmp = i;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }
      
    stream.writeByte((byte)len);
      
    len = (len < -120) ? -(len + 120) : -(len + 112);
      
    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      stream.writeByte((byte)((i & mask) >> shiftbits));
    }
  }
  

  /**
   * Reads a zero-compressed encoded long from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException 
   * @return deserialized long from stream.
   */
  public static long readVLong(DataInput stream) throws IOException {
    byte firstByte = stream.readByte();
    int len = decodeVIntSize(firstByte);
    if (len == 1) {
      return firstByte;
    }
    long i = 0;
    for (int idx = 0; idx < len-1; idx++) {
      byte b = stream.readByte();
      i = i << 8;
      i = i | (b & 0xFF);
    }
    return (isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  /**
   * Reads a zero-compressed encoded integer from input stream and returns it.
   * @param stream Binary input stream
   * @throws java.io.IOException 
   * @return deserialized integer from stream.
   */
  public static int readVInt(DataInput stream) throws IOException {
    return (int) readVLong(stream);
  }
 
  /**
   * Given the first byte of a vint/vlong, determine the sign
   * @param value the first byte
   * @return is the value negative
   */
  public static boolean isNegativeVInt(byte value) {
    return value < -120 || (value >= -112 && value < 0);
  }

  /**
   * Parse the first byte of a vint/vlong to determine the number of bytes
   * @param value the first byte of the vint/vlong
   * @return the total number of bytes (1 to 9)
   */
  public static int decodeVIntSize(byte value) {
    if (value >= -112) {
      return 1;
    } else if (value < -120) {
      return -119 - value;
    }
    return -111 - value;
  }

  /**
   * Get the encoded length if an integer is stored in a variable-length format
   * @return the encoded length 
   */
  public static int getVIntSize(long i) {
    if (i >= -112 && i <= 127) {
      return 1;
    }
      
    if (i < 0) {
      i ^= -1L; // take one's complement'
    }
    // find the number of bytes with non-leading zeros
    int dataBits = Long.SIZE - Long.numberOfLeadingZeros(i);
    // find the number of data bytes + length byte
    return (dataBits + 7) / 8 + 1;
  }
  /**
   * Read an Enum value from DataInput, Enums are read and written 
   * using String values. 
   * @param <T> Enum type
   * @param in DataInput to read from 
   * @param enumType Class type of Enum
   * @return Enum represented by String read from DataInput
   * @throws IOException
   */
  public static <T extends Enum<T>> T readEnum(DataInput in, Class<T> enumType)
    throws IOException{
    return T.valueOf(enumType, Text.readString(in));
  }
  /**
   * writes String value of enum to DataOutput. 
   * @param out Dataoutput stream
   * @param enumVal enum value
   * @throws IOException
   */
  public static void writeEnum(DataOutput out,  Enum<?> enumVal) 
    throws IOException{
    Text.writeString(out, enumVal.name()); 
  }
  /**
   * Skip <i>len</i> number of bytes in input stream<i>in</i>
   * @param in input stream
   * @param len number of bytes to skip
   * @throws IOException when skipped less number of bytes
   */
  public static void skipFully(DataInput in, int len) throws IOException {
    int total = 0;
    int cur = 0;

    while ((total<len) && ((cur = in.skipBytes(len-total)) > 0)) {
        total += cur;
    }

    if (total<len) {
      throw new IOException("Not able to skip " + len + " bytes, possibly " +
                            "due to end of input.");
    }
  }

  /** Convert writables to a byte array */
  public static byte[] toByteArray(Writable... writables) {
    final DataOutputBuffer out = new DataOutputBuffer();
    try {
      for(Writable w : writables) {
        w.write(out);
      }
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Fail to convert writables to a byte array",e);
    }
    return out.getData();
  }
}
