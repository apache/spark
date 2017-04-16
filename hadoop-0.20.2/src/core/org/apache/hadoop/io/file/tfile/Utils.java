/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.Text;

/**
 * Supporting Utility classes used by TFile, and shared by users of TFile.
 */
public final class Utils {

  /**
   * Prevent the instantiation of Utils.
   */
  private Utils() {
    // nothing
  }

  /**
   * Encoding an integer into a variable-length encoding format. Synonymous to
   * <code>Utils#writeVLong(out, n)</code>.
   * 
   * @param out
   *          output stream
   * @param n
   *          The integer to be encoded
   * @throws IOException
   * @see Utils#writeVLong(DataOutput, long)
   */
  public static void writeVInt(DataOutput out, int n) throws IOException {
    writeVLong(out, n);
  }

  /**
   * Encoding a Long integer into a variable-length encoding format.
   * <ul>
   * <li>if n in [-32, 127): encode in one byte with the actual value.
   * Otherwise,
   * <li>if n in [-20*2^8, 20*2^8): encode in two bytes: byte[0] = n/256 - 52;
   * byte[1]=n&0xff. Otherwise,
   * <li>if n IN [-16*2^16, 16*2^16): encode in three bytes: byte[0]=n/2^16 -
   * 88; byte[1]=(n>>8)&0xff; byte[2]=n&0xff. Otherwise,
   * <li>if n in [-8*2^24, 8*2^24): encode in four bytes: byte[0]=n/2^24 - 112;
   * byte[1] = (n>>16)&0xff; byte[2] = (n>>8)&0xff; byte[3]=n&0xff. Otherwise:
   * <li>if n in [-2^31, 2^31): encode in five bytes: byte[0]=-125; byte[1] =
   * (n>>24)&0xff; byte[2]=(n>>16)&0xff; byte[3]=(n>>8)&0xff; byte[4]=n&0xff;
   * <li>if n in [-2^39, 2^39): encode in six bytes: byte[0]=-124; byte[1] =
   * (n>>32)&0xff; byte[2]=(n>>24)&0xff; byte[3]=(n>>16)&0xff;
   * byte[4]=(n>>8)&0xff; byte[5]=n&0xff
   * <li>if n in [-2^47, 2^47): encode in seven bytes: byte[0]=-123; byte[1] =
   * (n>>40)&0xff; byte[2]=(n>>32)&0xff; byte[3]=(n>>24)&0xff;
   * byte[4]=(n>>16)&0xff; byte[5]=(n>>8)&0xff; byte[6]=n&0xff;
   * <li>if n in [-2^55, 2^55): encode in eight bytes: byte[0]=-122; byte[1] =
   * (n>>48)&0xff; byte[2] = (n>>40)&0xff; byte[3]=(n>>32)&0xff;
   * byte[4]=(n>>24)&0xff; byte[5]=(n>>16)&0xff; byte[6]=(n>>8)&0xff;
   * byte[7]=n&0xff;
   * <li>if n in [-2^63, 2^63): encode in nine bytes: byte[0]=-121; byte[1] =
   * (n>>54)&0xff; byte[2] = (n>>48)&0xff; byte[3] = (n>>40)&0xff;
   * byte[4]=(n>>32)&0xff; byte[5]=(n>>24)&0xff; byte[6]=(n>>16)&0xff;
   * byte[7]=(n>>8)&0xff; byte[8]=n&0xff;
   * </ul>
   * 
   * @param out
   *          output stream
   * @param n
   *          the integer number
   * @throws IOException
   */
  @SuppressWarnings("fallthrough")
  public static void writeVLong(DataOutput out, long n) throws IOException {
    if ((n < 128) && (n >= -32)) {
      out.writeByte((int) n);
      return;
    }

    long un = (n < 0) ? ~n : n;
    // how many bytes do we need to represent the number with sign bit?
    int len = (Long.SIZE - Long.numberOfLeadingZeros(un)) / 8 + 1;
    int firstByte = (int) (n >> ((len - 1) * 8));
    switch (len) {
      case 1:
        // fall it through to firstByte==-1, len=2.
        firstByte >>= 8;
      case 2:
        if ((firstByte < 20) && (firstByte >= -20)) {
          out.writeByte(firstByte - 52);
          out.writeByte((int) n);
          return;
        }
        // fall it through to firstByte==0/-1, len=3.
        firstByte >>= 8;
      case 3:
        if ((firstByte < 16) && (firstByte >= -16)) {
          out.writeByte(firstByte - 88);
          out.writeShort((int) n);
          return;
        }
        // fall it through to firstByte==0/-1, len=4.
        firstByte >>= 8;
      case 4:
        if ((firstByte < 8) && (firstByte >= -8)) {
          out.writeByte(firstByte - 112);
          out.writeShort(((int) n) >>> 8);
          out.writeByte((int) n);
          return;
        }
        out.writeByte(len - 129);
        out.writeInt((int) n);
        return;
      case 5:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 8));
        out.writeByte((int) n);
        return;
      case 6:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 16));
        out.writeShort((int) n);
        return;
      case 7:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 24));
        out.writeShort((int) (n >>> 8));
        out.writeByte((int) n);
        return;
      case 8:
        out.writeByte(len - 129);
        out.writeLong(n);
        return;
      default:
        throw new RuntimeException("Internel error");
    }
  }

  /**
   * Decoding the variable-length integer. Synonymous to
   * <code>(int)Utils#readVLong(in)</code>.
   * 
   * @param in
   *          input stream
   * @return the decoded integer
   * @throws IOException
   * 
   * @see Utils#readVLong(DataInput)
   */
  public static int readVInt(DataInput in) throws IOException {
    long ret = readVLong(in);
    if ((ret > Integer.MAX_VALUE) || (ret < Integer.MIN_VALUE)) {
      throw new RuntimeException(
          "Number too large to be represented as Integer");
    }
    return (int) ret;
  }

  /**
   * Decoding the variable-length integer. Suppose the value of the first byte
   * is FB, and the following bytes are NB[*].
   * <ul>
   * <li>if (FB >= -32), return (long)FB;
   * <li>if (FB in [-72, -33]), return (FB+52)<<8 + NB[0]&0xff;
   * <li>if (FB in [-104, -73]), return (FB+88)<<16 + (NB[0]&0xff)<<8 +
   * NB[1]&0xff;
   * <li>if (FB in [-120, -105]), return (FB+112)<<24 + (NB[0]&0xff)<<16 +
   * (NB[1]&0xff)<<8 + NB[2]&0xff;
   * <li>if (FB in [-128, -121]), return interpret NB[FB+129] as a signed
   * big-endian integer.
   * 
   * @param in
   *          input stream
   * @return the decoded long integer.
   * @throws IOException
   */

  public static long readVLong(DataInput in) throws IOException {
    int firstByte = in.readByte();
    if (firstByte >= -32) {
      return firstByte;
    }

    switch ((firstByte + 128) / 8) {
      case 11:
      case 10:
      case 9:
      case 8:
      case 7:
        return ((firstByte + 52) << 8) | in.readUnsignedByte();
      case 6:
      case 5:
      case 4:
      case 3:
        return ((firstByte + 88) << 16) | in.readUnsignedShort();
      case 2:
      case 1:
        return ((firstByte + 112) << 24) | (in.readUnsignedShort() << 8)
            | in.readUnsignedByte();
      case 0:
        int len = firstByte + 129;
        switch (len) {
          case 4:
            return in.readInt();
          case 5:
            return ((long) in.readInt()) << 8 | in.readUnsignedByte();
          case 6:
            return ((long) in.readInt()) << 16 | in.readUnsignedShort();
          case 7:
            return ((long) in.readInt()) << 24 | (in.readUnsignedShort() << 8)
                | in.readUnsignedByte();
          case 8:
            return in.readLong();
          default:
            throw new IOException("Corrupted VLong encoding");
        }
      default:
        throw new RuntimeException("Internal error");
    }
  }

  /**
   * Write a String as a VInt n, followed by n Bytes as in Text format.
   * 
   * @param out
   * @param s
   * @throws IOException
   */
  public static void writeString(DataOutput out, String s) throws IOException {
    if (s != null) {
      Text text = new Text(s);
      byte[] buffer = text.getBytes();
      int len = text.getLength();
      writeVInt(out, len);
      out.write(buffer, 0, len);
    } else {
      writeVInt(out, -1);
    }
  }

  /**
   * Read a String as a VInt n, followed by n Bytes in Text format.
   * 
   * @param in
   *          The input stream.
   * @return The string
   * @throws IOException
   */
  public static String readString(DataInput in) throws IOException {
    int length = readVInt(in);
    if (length == -1) return null;
    byte[] buffer = new byte[length];
    in.readFully(buffer);
    return Text.decode(buffer);
  }

  /**
   * A generic Version class. We suggest applications built on top of TFile use
   * this class to maintain version information in their meta blocks.
   * 
   * A version number consists of a major version and a minor version. The
   * suggested usage of major and minor version number is to increment major
   * version number when the new storage format is not backward compatible, and
   * increment the minor version otherwise.
   */
  public static final class Version implements Comparable<Version> {
    private final short major;
    private final short minor;

    /**
     * Construct the Version object by reading from the input stream.
     * 
     * @param in
     *          input stream
     * @throws IOException
     */
    public Version(DataInput in) throws IOException {
      major = in.readShort();
      minor = in.readShort();
    }

    /**
     * Constructor.
     * 
     * @param major
     *          major version.
     * @param minor
     *          minor version.
     */
    public Version(short major, short minor) {
      this.major = major;
      this.minor = minor;
    }

    /**
     * Write the objec to a DataOutput. The serialized format of the Version is
     * major version followed by minor version, both as big-endian short
     * integers.
     * 
     * @param out
     *          The DataOutput object.
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
      out.writeShort(major);
      out.writeShort(minor);
    }

    /**
     * Get the major version.
     * 
     * @return Major version.
     */
    public int getMajor() {
      return major;
    }

    /**
     * Get the minor version.
     * 
     * @return The minor version.
     */
    public int getMinor() {
      return minor;
    }

    /**
     * Get the size of the serialized Version object.
     * 
     * @return serialized size of the version object.
     */
    public static int size() {
      return (Short.SIZE + Short.SIZE) / Byte.SIZE;
    }

    /**
     * Return a string representation of the version.
     */
    public String toString() {
      return new StringBuilder("v").append(major).append(".").append(minor)
          .toString();
    }

    /**
     * Test compatibility.
     * 
     * @param other
     *          The Version object to test compatibility with.
     * @return true if both versions have the same major version number; false
     *         otherwise.
     */
    public boolean compatibleWith(Version other) {
      return major == other.major;
    }

    /**
     * Compare this version with another version.
     */
    @Override
    public int compareTo(Version that) {
      if (major != that.major) {
        return major - that.major;
      }
      return minor - that.minor;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) return true;
      if (!(other instanceof Version)) return false;
      return compareTo((Version) other) == 0;
    }

    @Override
    public int hashCode() {
      return (major << 16 + minor);
    }
  }

  /**
   * Lower bound binary search. Find the index to the first element in the list
   * that compares greater than or equal to key.
   * 
   * @param <T>
   *          Type of the input key.
   * @param list
   *          The list
   * @param key
   *          The input key.
   * @param cmp
   *          Comparator for the key.
   * @return The index to the desired element if it exists; or list.size()
   *         otherwise.
   */
  public static <T> int lowerBound(List<? extends T> list, T key,
      Comparator<? super T> cmp) {
    int low = 0;
    int high = list.size();

    while (low < high) {
      int mid = (low + high) >>> 1;
      T midVal = list.get(mid);
      int ret = cmp.compare(midVal, key);
      if (ret < 0)
        low = mid + 1;
      else high = mid;
    }
    return low;
  }

  /**
   * Upper bound binary search. Find the index to the first element in the list
   * that compares greater than the input key.
   * 
   * @param <T>
   *          Type of the input key.
   * @param list
   *          The list
   * @param key
   *          The input key.
   * @param cmp
   *          Comparator for the key.
   * @return The index to the desired element if it exists; or list.size()
   *         otherwise.
   */
  public static <T> int upperBound(List<? extends T> list, T key,
      Comparator<? super T> cmp) {
    int low = 0;
    int high = list.size();

    while (low < high) {
      int mid = (low + high) >>> 1;
      T midVal = list.get(mid);
      int ret = cmp.compare(midVal, key);
      if (ret <= 0)
        low = mid + 1;
      else high = mid;
    }
    return low;
  }

  /**
   * Lower bound binary search. Find the index to the first element in the list
   * that compares greater than or equal to key.
   * 
   * @param <T>
   *          Type of the input key.
   * @param list
   *          The list
   * @param key
   *          The input key.
   * @return The index to the desired element if it exists; or list.size()
   *         otherwise.
   */
  public static <T> int lowerBound(List<? extends Comparable<? super T>> list,
      T key) {
    int low = 0;
    int high = list.size();

    while (low < high) {
      int mid = (low + high) >>> 1;
      Comparable<? super T> midVal = list.get(mid);
      int ret = midVal.compareTo(key);
      if (ret < 0)
        low = mid + 1;
      else high = mid;
    }
    return low;
  }

  /**
   * Upper bound binary search. Find the index to the first element in the list
   * that compares greater than the input key.
   * 
   * @param <T>
   *          Type of the input key.
   * @param list
   *          The list
   * @param key
   *          The input key.
   * @return The index to the desired element if it exists; or list.size()
   *         otherwise.
   */
  public static <T> int upperBound(List<? extends Comparable<? super T>> list,
      T key) {
    int low = 0;
    int high = list.size();

    while (low < high) {
      int mid = (low + high) >>> 1;
      Comparable<? super T> midVal = list.get(mid);
      int ret = midVal.compareTo(key);
      if (ret <= 0)
        low = mid + 1;
      else high = mid;
    }
    return low;
  }
}
