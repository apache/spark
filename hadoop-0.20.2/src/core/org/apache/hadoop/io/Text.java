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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.MalformedInputException;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** This class stores text using standard UTF8 encoding.  It provides methods
 * to serialize, deserialize, and compare texts at byte level.  The type of
 * length is integer and is serialized using zero-compressed format.  <p>In
 * addition, it provides methods for string traversal without converting the
 * byte array to a string.  <p>Also includes utilities for
 * serializing/deserialing a string, coding/decoding a string, checking if a
 * byte array contains valid UTF8 code, calculating the length of an encoded
 * string.
 */
public class Text extends BinaryComparable
    implements WritableComparable<BinaryComparable> {
  private static final Log LOG= LogFactory.getLog(Text.class);
  
  private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
    new ThreadLocal<CharsetEncoder>() {
      protected CharsetEncoder initialValue() {
        return Charset.forName("UTF-8").newEncoder().
               onMalformedInput(CodingErrorAction.REPORT).
               onUnmappableCharacter(CodingErrorAction.REPORT);
    }
  };
  
  private static ThreadLocal<CharsetDecoder> DECODER_FACTORY =
    new ThreadLocal<CharsetDecoder>() {
    protected CharsetDecoder initialValue() {
      return Charset.forName("UTF-8").newDecoder().
             onMalformedInput(CodingErrorAction.REPORT).
             onUnmappableCharacter(CodingErrorAction.REPORT);
    }
  };
  
  private static final byte [] EMPTY_BYTES = new byte[0];
  
  private byte[] bytes;
  private int length;

  public Text() {
    bytes = EMPTY_BYTES;
  }

  /** Construct from a string. 
   */
  public Text(String string) {
    set(string);
  }

  /** Construct from another text. */
  public Text(Text utf8) {
    set(utf8);
  }

  /** Construct from a byte array.
   */
  public Text(byte[] utf8)  {
    set(utf8);
  }
  
  /**
   * Returns the raw bytes; however, only data up to {@link #getLength()} is
   * valid.
   */
  public byte[] getBytes() {
    return bytes;
  }

  /** Returns the number of bytes in the byte array */ 
  public int getLength() {
    return length;
  }
  
  /**
   * Returns the Unicode Scalar Value (32-bit integer value)
   * for the character at <code>position</code>. Note that this
   * method avoids using the converter or doing String instatiation
   * @return the Unicode scalar value at position or -1
   *          if the position is invalid or points to a
   *          trailing byte
   */
  public int charAt(int position) {
    if (position > this.length) return -1; // too long
    if (position < 0) return -1; // duh.
      
    ByteBuffer bb = (ByteBuffer)ByteBuffer.wrap(bytes).position(position);
    return bytesToCodePoint(bb.slice());
  }
  
  public int find(String what) {
    return find(what, 0);
  }
  
  /**
   * Finds any occurence of <code>what</code> in the backing
   * buffer, starting as position <code>start</code>. The starting
   * position is measured in bytes and the return value is in
   * terms of byte position in the buffer. The backing buffer is
   * not converted to a string for this operation.
   * @return byte position of the first occurence of the search
   *         string in the UTF-8 buffer or -1 if not found
   */
  public int find(String what, int start) {
    try {
      ByteBuffer src = ByteBuffer.wrap(this.bytes,0,this.length);
      ByteBuffer tgt = encode(what);
      byte b = tgt.get();
      src.position(start);
          
      while (src.hasRemaining()) {
        if (b == src.get()) { // matching first byte
          src.mark(); // save position in loop
          tgt.mark(); // save position in target
          boolean found = true;
          int pos = src.position()-1;
          while (tgt.hasRemaining()) {
            if (!src.hasRemaining()) { // src expired first
              tgt.reset();
              src.reset();
              found = false;
              break;
            }
            if (!(tgt.get() == src.get())) {
              tgt.reset();
              src.reset();
              found = false;
              break; // no match
            }
          }
          if (found) return pos;
        }
      }
      return -1; // not found
    } catch (CharacterCodingException e) {
      // can't get here
      e.printStackTrace();
      return -1;
    }
  }  
  /** Set to contain the contents of a string. 
   */
  public void set(String string) {
    try {
      ByteBuffer bb = encode(string, true);
      bytes = bb.array();
      length = bb.limit();
    }catch(CharacterCodingException e) {
      throw new RuntimeException("Should not have happened " + e.toString()); 
    }
  }

  /** Set to a utf8 byte array
   */
  public void set(byte[] utf8) {
    set(utf8, 0, utf8.length);
  }
  
  /** copy a text. */
  public void set(Text other) {
    set(other.getBytes(), 0, other.getLength());
  }

  /**
   * Set the Text to range of bytes
   * @param utf8 the data to copy from
   * @param start the first position of the new string
   * @param len the number of bytes of the new string
   */
  public void set(byte[] utf8, int start, int len) {
    setCapacity(len, false);
    System.arraycopy(utf8, start, bytes, 0, len);
    this.length = len;
  }

  /**
   * Append a range of bytes to the end of the given text
   * @param utf8 the data to copy from
   * @param start the first position to append from utf8
   * @param len the number of bytes to append
   */
  public void append(byte[] utf8, int start, int len) {
    setCapacity(length + len, true);
    System.arraycopy(utf8, start, bytes, length, len);
    length += len;
  }

  /**
   * Clear the string to empty.
   */
  public void clear() {
    length = 0;
  }

  /*
   * Sets the capacity of this Text object to <em>at least</em>
   * <code>len</code> bytes. If the current buffer is longer,
   * then the capacity and existing content of the buffer are
   * unchanged. If <code>len</code> is larger
   * than the current capacity, the Text object's capacity is
   * increased to match.
   * @param len the number of bytes we need
   * @param keepData should the old data be kept
   */
  private void setCapacity(int len, boolean keepData) {
    if (bytes == null || bytes.length < len) {
      byte[] newBytes = new byte[len];
      if (bytes != null && keepData) {
        System.arraycopy(bytes, 0, newBytes, 0, length);
      }
      bytes = newBytes;
    }
  }
   
  /** 
   * Convert text back to string
   * @see java.lang.Object#toString()
   */
  public String toString() {
    try {
      return decode(bytes, 0, length);
    } catch (CharacterCodingException e) { 
      throw new RuntimeException("Should not have happened " + e.toString()); 
    }
  }
  
  /** deserialize 
   */
  public void readFields(DataInput in) throws IOException {
    int newLength = WritableUtils.readVInt(in);
    setCapacity(newLength, false);
    in.readFully(bytes, 0, newLength);
    length = newLength;
  }

  /** Skips over one Text in the input. */
  public static void skip(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    WritableUtils.skipFully(in, length);
  }

  /** serialize
   * write this object to out
   * length uses zero-compressed encoding
   * @see Writable#write(DataOutput)
   */
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, length);
    out.write(bytes, 0, length);
  }

  /** Returns true iff <code>o</code> is a Text with the same contents.  */
  public boolean equals(Object o) {
    if (o instanceof Text)
      return super.equals(o);
    return false;
  }

  public int hashCode() {
    return super.hashCode();
  }

  /** A WritableComparator optimized for Text keys. */
  public static class Comparator extends WritableComparator {
    public Comparator() {
      super(Text.class);
    }

    public int compare(byte[] b1, int s1, int l1,
                       byte[] b2, int s2, int l2) {
      int n1 = WritableUtils.decodeVIntSize(b1[s1]);
      int n2 = WritableUtils.decodeVIntSize(b2[s2]);
      return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2);
    }
  }

  static {
    // register this comparator
    WritableComparator.define(Text.class, new Comparator());
  }

  /// STATIC UTILITIES FROM HERE DOWN
  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If the input is malformed,
   * replace by a default value.
   */
  public static String decode(byte[] utf8) throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8), true);
  }
  
  public static String decode(byte[] utf8, int start, int length) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), true);
  }
  
  /**
   * Converts the provided byte array to a String using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   */
  public static String decode(byte[] utf8, int start, int length, boolean replace) 
    throws CharacterCodingException {
    return decode(ByteBuffer.wrap(utf8, start, length), replace);
  }
  
  private static String decode(ByteBuffer utf8, boolean replace) 
    throws CharacterCodingException {
    CharsetDecoder decoder = DECODER_FACTORY.get();
    if (replace) {
      decoder.onMalformedInput(
          java.nio.charset.CodingErrorAction.REPLACE);
      decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    String str = decoder.decode(utf8).toString();
    // set decoder back to its default value: REPORT
    if (replace) {
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return str;
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If the input is malformed,
   * invalid chars are replaced by a default value.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */

  public static ByteBuffer encode(String string)
    throws CharacterCodingException {
    return encode(string, true);
  }

  /**
   * Converts the provided String to bytes using the
   * UTF-8 encoding. If <code>replace</code> is true, then
   * malformed input is replaced with the
   * substitution character, which is U+FFFD. Otherwise the
   * method throws a MalformedInputException.
   * @return ByteBuffer: bytes stores at ByteBuffer.array() 
   *                     and length is ByteBuffer.limit()
   */
  public static ByteBuffer encode(String string, boolean replace)
    throws CharacterCodingException {
    CharsetEncoder encoder = ENCODER_FACTORY.get();
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPLACE);
      encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
    ByteBuffer bytes = 
      encoder.encode(CharBuffer.wrap(string.toCharArray()));
    if (replace) {
      encoder.onMalformedInput(CodingErrorAction.REPORT);
      encoder.onUnmappableCharacter(CodingErrorAction.REPORT);
    }
    return bytes;
  }

  /** Read a UTF8 encoded string from in
   */
  public static String readString(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte [] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    return decode(bytes);
  }

  /** Write a UTF8 encoded string to out
   */
  public static int writeString(DataOutput out, String s) throws IOException {
    ByteBuffer bytes = encode(s);
    int length = bytes.limit();
    WritableUtils.writeVInt(out, length);
    out.write(bytes.array(), 0, length);
    return length;
  }

  ////// states for validateUTF8
  
  private static final int LEAD_BYTE = 0;

  private static final int TRAIL_BYTE_1 = 1;

  private static final int TRAIL_BYTE = 2;

  /** 
   * Check if a byte array contains valid utf-8
   * @param utf8 byte array
   * @throws MalformedInputException if the byte array contains invalid utf-8
   */
  public static void validateUTF8(byte[] utf8) throws MalformedInputException {
    validateUTF8(utf8, 0, utf8.length);     
  }
  
  /**
   * Check to see if a byte array is valid utf-8
   * @param utf8 the array of bytes
   * @param start the offset of the first byte in the array
   * @param len the length of the byte sequence
   * @throws MalformedInputException if the byte array contains invalid bytes
   */
  public static void validateUTF8(byte[] utf8, int start, int len)
    throws MalformedInputException {
    int count = start;
    int leadByte = 0;
    int length = 0;
    int state = LEAD_BYTE;
    while (count < start+len) {
      int aByte = ((int) utf8[count] & 0xFF);

      switch (state) {
      case LEAD_BYTE:
        leadByte = aByte;
        length = bytesFromUTF8[aByte];

        switch (length) {
        case 0: // check for ASCII
          if (leadByte > 0x7F)
            throw new MalformedInputException(count);
          break;
        case 1:
          if (leadByte < 0xC2 || leadByte > 0xDF)
            throw new MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        case 2:
          if (leadByte < 0xE0 || leadByte > 0xEF)
            throw new MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        case 3:
          if (leadByte < 0xF0 || leadByte > 0xF4)
            throw new MalformedInputException(count);
          state = TRAIL_BYTE_1;
          break;
        default:
          // too long! Longest valid UTF-8 is 4 bytes (lead + three)
          // or if < 0 we got a trail byte in the lead byte position
          throw new MalformedInputException(count);
        } // switch (length)
        break;

      case TRAIL_BYTE_1:
        if (leadByte == 0xF0 && aByte < 0x90)
          throw new MalformedInputException(count);
        if (leadByte == 0xF4 && aByte > 0x8F)
          throw new MalformedInputException(count);
        if (leadByte == 0xE0 && aByte < 0xA0)
          throw new MalformedInputException(count);
        if (leadByte == 0xED && aByte > 0x9F)
          throw new MalformedInputException(count);
        // falls through to regular trail-byte test!!
      case TRAIL_BYTE:
        if (aByte < 0x80 || aByte > 0xBF)
          throw new MalformedInputException(count);
        if (--length == 0) {
          state = LEAD_BYTE;
        } else {
          state = TRAIL_BYTE;
        }
        break;
      } // switch (state)
      count++;
    }
  }

  /**
   * Magic numbers for UTF-8. These are the number of bytes
   * that <em>follow</em> a given lead byte. Trailing bytes
   * have the value -1. The values 4 and 5 are presented in
   * this table, even though valid UTF-8 cannot include the
   * five and six byte sequences.
   */
  static final int[] bytesFromUTF8 =
  { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0,
    // trail bytes
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
    -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3,
    3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5 };

  /**
   * Returns the next code point at the current position in
   * the buffer. The buffer's position will be incremented.
   * Any mark set on this buffer will be changed by this method!
   */
  public static int bytesToCodePoint(ByteBuffer bytes) {
    bytes.mark();
    byte b = bytes.get();
    bytes.reset();
    int extraBytesToRead = bytesFromUTF8[(b & 0xFF)];
    if (extraBytesToRead < 0) return -1; // trailing byte!
    int ch = 0;

    switch (extraBytesToRead) {
    case 5: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
    case 4: ch += (bytes.get() & 0xFF); ch <<= 6; /* remember, illegal UTF-8 */
    case 3: ch += (bytes.get() & 0xFF); ch <<= 6;
    case 2: ch += (bytes.get() & 0xFF); ch <<= 6;
    case 1: ch += (bytes.get() & 0xFF); ch <<= 6;
    case 0: ch += (bytes.get() & 0xFF);
    }
    ch -= offsetsFromUTF8[extraBytesToRead];

    return ch;
  }

  
  static final int offsetsFromUTF8[] =
  { 0x00000000, 0x00003080,
    0x000E2080, 0x03C82080, 0xFA082080, 0x82082080 };

  /**
   * For the given string, returns the number of UTF-8 bytes
   * required to encode the string.
   * @param string text to encode
   * @return number of UTF-8 bytes required to encode
   */
  public static int utf8Length(String string) {
    CharacterIterator iter = new StringCharacterIterator(string);
    char ch = iter.first();
    int size = 0;
    while (ch != CharacterIterator.DONE) {
      if ((ch >= 0xD800) && (ch < 0xDC00)) {
        // surrogate pair?
        char trail = iter.next();
        if ((trail > 0xDBFF) && (trail < 0xE000)) {
          // valid pair
          size += 4;
        } else {
          // invalid pair
          size += 3;
          iter.previous(); // rewind one
        }
      } else if (ch < 0x80) {
        size++;
      } else if (ch < 0x800) {
        size += 2;
      } else {
        // ch < 0x10000, that is, the largest char value
        size += 3;
      }
      ch = iter.next();
    }
    return size;
  }
}
