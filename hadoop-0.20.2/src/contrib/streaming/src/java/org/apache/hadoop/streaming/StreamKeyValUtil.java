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

public class StreamKeyValUtil {

  /**
   * Find the first occured tab in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @param start starting offset
   * @param length no. of bytes
   * @return position that first tab occures otherwise -1
   */
  public static int findTab(byte [] utf, int start, int length) {
    for(int i=start; i<(start+length); i++) {
      if (utf[i]==(byte)'\t') {
        return i;
      }
    }
    return -1;      
  }
  /**
   * Find the first occured tab in a UTF-8 encoded string
   * @param utf a byte array containing a UTF-8 encoded string
   * @return position that first tab occures otherwise -1
   */
  public static int findTab(byte [] utf) {
    return org.apache.hadoop.util.UTF8ByteArrayUtils.findNthByte(utf, 0, 
        utf.length, (byte)'\t', 1);
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
   * @throws IOException
   */
  public static void splitKeyVal(byte[] utf, int start, int length, 
                                 Text key, Text val, int splitPos,
                                 int separatorLength) throws IOException {
    if (splitPos<start || splitPos >= (start+length))
      throw new IllegalArgumentException("splitPos must be in the range " +
                                         "[" + start + ", " + (start+length) + "]: " + splitPos);
    int keyLen = (splitPos-start);
    byte [] keyBytes = new byte[keyLen];
    System.arraycopy(utf, start, keyBytes, 0, keyLen);
    int valLen = (start+length)-splitPos-separatorLength;
    byte [] valBytes = new byte[valLen];
    System.arraycopy(utf, splitPos+separatorLength, valBytes, 0, valLen);
    key.set(keyBytes);
    val.set(valBytes);
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
   * @throws IOException
   */
  public static void splitKeyVal(byte[] utf, int start, int length, 
                                 Text key, Text val, int splitPos) throws IOException {
    splitKeyVal(utf, start, length, key, val, splitPos, 1);
  }
  

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @param separatorLength the length of the separator between key and value
   * @throws IOException
   */
  public static void splitKeyVal(byte[] utf, Text key, Text val, int splitPos, 
                                 int separatorLength) 
    throws IOException {
    splitKeyVal(utf, 0, utf.length, key, val, splitPos, separatorLength);
  }

  /**
   * split a UTF-8 byte array into key and value 
   * assuming that the delimilator is at splitpos. 
   * @param utf utf-8 encoded string
   * @param key contains key upon the method is returned
   * @param val contains value upon the method is returned
   * @param splitPos the split pos
   * @throws IOException
   */
  public static void splitKeyVal(byte[] utf, Text key, Text val, int splitPos) 
    throws IOException {
    splitKeyVal(utf, 0, utf.length, key, val, splitPos, 1);
  }
  
  /**
   * Read a utf8 encoded line from a data input stream. 
   * @param lineReader LineReader to read the line from.
   * @param out Text to read into
   * @return number of bytes read 
   * @throws IOException
   */
  public static int readLine(LineReader lineReader, Text out) 
  throws IOException {
    out.clear();
    return lineReader.readLine(out);
  }

}
