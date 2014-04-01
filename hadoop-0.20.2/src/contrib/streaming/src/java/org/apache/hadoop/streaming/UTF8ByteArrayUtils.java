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

package org.apache.hadoop.streaming;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

/**
 * General utils for byte array containing UTF-8 encoded strings
 * @deprecated use {@link org.apache.hadoop.util.UTF8ByteArrayUtils} and
 * {@link StreamKeyValUtil} instead
 */

public class UTF8ByteArrayUtils {
  /**
   * Find the first occured tab in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param length no. of bytes
   * @return position that first tab occures otherwise -1
   * @deprecated use {@link StreamKeyValUtil#findTab(byte[], int, int)}
   */
  @Deprecated
  public static int findTab(byte [] utf, int start, int length) {
    return StreamKeyValUtil.findTab(utf, start, length);      
  }
  
  /**
   * Find the first occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param end ending position
   * @param b the byte to find
   * @return position that first byte occures otherwise -1
   * @deprecated use 
   * {@link org.apache.hadoop.util.UTF8ByteArrayUtils#findByte(byte[], int,
   *  int, byte)}
   */
  @Deprecated
  public static int findByte(byte [] utf, int start, int end, byte b) {
    return org.apache.hadoop.util.UTF8ByteArrayUtils.findByte(utf, start, end, b);
  }

  /**
   * Find the first occurrence of the given bytes b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param end ending position
   * @param b the bytes to find
   * @return position that first byte occures otherwise -1
   * @deprecated use 
   * {@link org.apache.hadoop.util.UTF8ByteArrayUtils#findBytes(byte[], int, 
   * int, byte[])}
   */
  @Deprecated
  public static int findBytes(byte [] utf, int start, int end, byte[] b) {
    return org.apache.hadoop.util.UTF8ByteArrayUtils.findBytes(utf, start, end, b);      
  }
    
  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param length the length of byte array
   * @param b the byte to find
   * @param n the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   * @deprecated use 
   * {@link org.apache.hadoop.util.UTF8ByteArrayUtils#findNthByte(byte[], int, 
   * int, byte, int)}
   */
  @Deprecated
  public static int findNthByte(byte [] utf, int start, int length, byte b, int n) {
    return org.apache.hadoop.util.UTF8ByteArrayUtils.findNthByte(utf, start,
        length, b, n);
  }
  
  /**
   * Find the nth occurrence of the given byte b in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param b the byte to find
   * @param n the desired occurrence of the given byte
   * @return position that nth occurrence of the given byte if exists; otherwise -1
   * @deprecated use 
   * {@link org.apache.hadoop.util.UTF8ByteArrayUtils#findNthByte(byte[], 
   * byte, int)}
   */
  @Deprecated
  public static int findNthByte(byte [] utf, byte b, int n) {
    return org.apache.hadoop.util.UTF8ByteArrayUtils.findNthByte(utf, b, n);      
  }
    
  /**
   * Find the first occured tab in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @return position that first tab occures otherwise -1
   * @deprecated use {@link StreamKeyValUtil#findTab(byte[])}
   */
  @Deprecated
  public static int findTab(byte [] utf) {
    return StreamKeyValUtil.findTab(utf);
  }

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param start starting offset
   * @param length no. of bytes
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @param separatorLength the length of the separator between key and value
   * @deprecated use 
   * {@link StreamKeyValUtil#splitKeyVal(byte[], int, int, Text, Text, 
   * int, int)}
   * @throws IOException
   */
  @Deprecated
  public static void splitKeyVal(byte[] utf, int start, int length, 
                                 Text key, Text val, int splitPos,
                                 int separatorLength) throws IOException {
    StreamKeyValUtil.splitKeyVal(utf, start, 
        length, key, val, splitPos, separatorLength);
  }

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param start starting offset
   * @param length no. of bytes
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @deprecated use 
   * {@link StreamKeyValUtil#splitKeyVal(byte[], int, int, Text, Text, int)}
   * @throws IOException
   */
  @Deprecated
  public static void splitKeyVal(byte[] utf, int start, int length, 
                                 Text key, Text val, int splitPos) throws IOException {
    StreamKeyValUtil.splitKeyVal(utf, start, length, key, val, splitPos);
  }
  

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @param separatorLength the length of the separator between key and value
   * @deprecated use 
   * {@link StreamKeyValUtil#splitKeyVal(byte[], Text, Text, int, int)}
   * @throws IOException
   */
  @Deprecated
  public static void splitKeyVal(byte[] utf, Text key, Text val, int splitPos, 
                                 int separatorLength) 
    throws IOException {
    StreamKeyValUtil.splitKeyVal(utf, key, val, splitPos, separatorLength);
  }

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @deprecated use 
   * {@link StreamKeyValUtil#splitKeyVal(byte[], Text, Text, int)}
   * @throws IOException
   */
  @Deprecated
  public static void splitKeyVal(byte[] utf, Text key, Text val, int splitPos) 
    throws IOException {
    StreamKeyValUtil.splitKeyVal(utf, key, val, splitPos);
  }
  
  /**
   * Read a utf8 encoded line from a data input stream. 
   * @param lineReader LineReader to read the line from.
   * @param out Text to read into
   * @return number of bytes read
   * @deprecated use 
   * {@link StreamKeyValUtil#readLine(LineReader, Text)} 
   * @throws IOException
   */
  @Deprecated
  public static int readLine(LineReader lineReader, Text out) 
  throws IOException {
    return StreamKeyValUtil.readLine(lineReader, out);
  }
}
