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

import java.io.IOException;

/**
 * Interface that all the Deserializers have to implement.
 */
public interface RecordInput {
  /**
   * Read a byte from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  byte readByte(String tag) throws IOException;
  
  /**
   * Read a boolean from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  boolean readBool(String tag) throws IOException;
  
  /**
   * Read an integer from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  int readInt(String tag) throws IOException;
  
  /**
   * Read a long integer from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  long readLong(String tag) throws IOException;
  
  /**
   * Read a single-precision float from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  float readFloat(String tag) throws IOException;
  
  /**
   * Read a double-precision number from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  double readDouble(String tag) throws IOException;
  
  /**
   * Read a UTF-8 encoded string from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  String readString(String tag) throws IOException;
  
  /**
   * Read byte array from serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return value read from serialized record.
   */
  Buffer readBuffer(String tag) throws IOException;
  
  /**
   * Check the mark for start of the serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   */
  void startRecord(String tag) throws IOException;
  
  /**
   * Check the mark for end of the serialized record.
   * @param tag Used by tagged serialization formats (such as XML)
   */
  void endRecord(String tag) throws IOException;
  
  /**
   * Check the mark for start of the serialized vector.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return Index that is used to count the number of elements.
   */
  Index startVector(String tag) throws IOException;
  
  /**
   * Check the mark for end of the serialized vector.
   * @param tag Used by tagged serialization formats (such as XML)
   */
  void endVector(String tag) throws IOException;
  
  /**
   * Check the mark for start of the serialized map.
   * @param tag Used by tagged serialization formats (such as XML)
   * @return Index that is used to count the number of map entries.
   */
  Index startMap(String tag) throws IOException;
  
  /**
   * Check the mark for end of the serialized map.
   * @param tag Used by tagged serialization formats (such as XML)
   */
  void endMap(String tag) throws IOException;
}
