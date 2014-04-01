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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStream;
import java.util.Arrays;
import java.security.*;

/** A Writable for MD5 hash values.
 */
public class MD5Hash implements WritableComparable<MD5Hash> {
  public static final int MD5_LEN = 16;

  private static ThreadLocal<MessageDigest> DIGESTER_FACTORY = new ThreadLocal<MessageDigest>() {
    protected MessageDigest initialValue() {
      try {
        return MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
    }
  };

  private byte[] digest;

  /** Constructs an MD5Hash. */
  public MD5Hash() {
    this.digest = new byte[MD5_LEN];
  }

  /** Constructs an MD5Hash from a hex string. */
  public MD5Hash(String hex) {
    setDigest(hex);
  }
  
  /** Constructs an MD5Hash with a specified value. */
  public MD5Hash(byte[] digest) {
    if (digest.length != MD5_LEN)
      throw new IllegalArgumentException("Wrong length: " + digest.length);
    this.digest = digest;
  }
  
  // javadoc from Writable
  public void readFields(DataInput in) throws IOException {
    in.readFully(digest);
  }

  /** Constructs, reads and returns an instance. */
  public static MD5Hash read(DataInput in) throws IOException {
    MD5Hash result = new MD5Hash();
    result.readFields(in);
    return result;
  }

  // javadoc from Writable
  public void write(DataOutput out) throws IOException {
    out.write(digest);
  }

  /** Copy the contents of another instance into this instance. */
  public void set(MD5Hash that) {
    System.arraycopy(that.digest, 0, this.digest, 0, MD5_LEN);
  }

  /** Returns the digest bytes. */
  public byte[] getDigest() { return digest; }

  /** Construct a hash value for a byte array. */
  public static MD5Hash digest(byte[] data) {
    return digest(data, 0, data.length);
  }
  
  /**
   * Create a thread local MD5 digester
   */
  public static MessageDigest getDigester() {
    return DIGESTER_FACTORY.get();
  }

  /** Construct a hash value for the content from the InputStream. */
  public static MD5Hash digest(InputStream in) throws IOException {
    final byte[] buffer = new byte[4*1024]; 

    final MessageDigest digester = getDigester();
    for(int n; (n = in.read(buffer)) != -1; ) {
      digester.update(buffer, 0, n);
    }

    return new MD5Hash(digester.digest());
  }

  /** Construct a hash value for a byte array. */
  public static MD5Hash digest(byte[] data, int start, int len) {
    byte[] digest;
    MessageDigest digester = getDigester();
    digester.update(data, start, len);
    digest = digester.digest();
    return new MD5Hash(digest);
  }

  /** Construct a hash value for a String. */
  public static MD5Hash digest(String string) {
    return digest(UTF8.getBytes(string));
  }

  /** Construct a hash value for a String. */
  public static MD5Hash digest(UTF8 utf8) {
    return digest(utf8.getBytes(), 0, utf8.getLength());
  }

  /** Construct a half-sized version of this MD5.  Fits in a long **/
  public long halfDigest() {
    long value = 0;
    for (int i = 0; i < 8; i++)
      value |= ((digest[i] & 0xffL) << (8*(7-i)));
    return value;
  }

  /**
   * Return a 32-bit digest of the MD5.
   * @return the first 4 bytes of the md5
   */
  public int quarterDigest() {
    int value = 0;
    for (int i = 0; i < 4; i++)
      value |= ((digest[i] & 0xff) << (8*(3-i)));
    return value;    
  }

  /** Returns true iff <code>o</code> is an MD5Hash whose digest contains the
   * same values.  */
  public boolean equals(Object o) {
    if (!(o instanceof MD5Hash))
      return false;
    MD5Hash other = (MD5Hash)o;
    return Arrays.equals(this.digest, other.digest);
  }

  /** Returns a hash code value for this object.
   * Only uses the first 4 bytes, since md5s are evenly distributed.
   */
  public int hashCode() {
    return quarterDigest();
  }


  /** Compares this object with the specified object for order.*/
  public int compareTo(MD5Hash that) {
    return WritableComparator.compareBytes(this.digest, 0, MD5_LEN,
                                           that.digest, 0, MD5_LEN);
  }

  /** A WritableComparator optimized for MD5Hash keys. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(MD5Hash.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      return compareBytes(b1, s1, MD5_LEN, b2, s2, MD5_LEN);
    }
  }

  static {                                        // register this comparator
    WritableComparator.define(MD5Hash.class, new Comparator());
  }

  private static final char[] HEX_DIGITS =
  {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};

  /** Returns a string representation of this object. */
  public String toString() {
    StringBuffer buf = new StringBuffer(MD5_LEN*2);
    for (int i = 0; i < MD5_LEN; i++) {
      int b = digest[i];
      buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
      buf.append(HEX_DIGITS[b & 0xf]);
    }
    return buf.toString();
  }

  /** Sets the digest value from a hex string. */
  public void setDigest(String hex) {
    if (hex.length() != MD5_LEN*2)
      throw new IllegalArgumentException("Wrong length: " + hex.length());
    byte[] digest = new byte[MD5_LEN];
    for (int i = 0; i < MD5_LEN; i++) {
      int j = i << 1;
      digest[i] = (byte)(charToNibble(hex.charAt(j)) << 4 |
                         charToNibble(hex.charAt(j+1)));
    }
    this.digest = digest;
  }

  private static final int charToNibble(char c) {
    if (c >= '0' && c <= '9') {
      return c - '0';
    } else if (c >= 'a' && c <= 'f') {
      return 0xa + (c - 'a');
    } else if (c >= 'A' && c <= 'F') {
      return 0xA + (c - 'A');
    } else {
      throw new RuntimeException("Not a hex character: " + c);
    }
  }


}
