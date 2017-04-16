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
import java.util.TreeMap;
import java.util.ArrayList;

/**
 * Interface that alll the serializers have to implement.
 */
public interface RecordOutput {
  /**
   * Write a byte to serialized record.
   * @param b Byte to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeByte(byte b, String tag) throws IOException;
  
  /**
   * Write a boolean to serialized record.
   * @param b Boolean to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeBool(boolean b, String tag) throws IOException;
  
  /**
   * Write an integer to serialized record.
   * @param i Integer to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeInt(int i, String tag) throws IOException;
  
  /**
   * Write a long integer to serialized record.
   * @param l Long to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeLong(long l, String tag) throws IOException;
  
  /**
   * Write a single-precision float to serialized record.
   * @param f Float to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeFloat(float f, String tag) throws IOException;
  
  /**
   * Write a double precision floating point number to serialized record.
   * @param d Double to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeDouble(double d, String tag) throws IOException;
  
  /**
   * Write a unicode string to serialized record.
   * @param s String to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeString(String s, String tag) throws IOException;
  
  /**
   * Write a buffer to serialized record.
   * @param buf Buffer to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void writeBuffer(Buffer buf, String tag)
    throws IOException;
  
  /**
   * Mark the start of a record to be serialized.
   * @param r Record to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startRecord(Record r, String tag) throws IOException;
  
  /**
   * Mark the end of a serialized record.
   * @param r Record to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endRecord(Record r, String tag) throws IOException;
  
  /**
   * Mark the start of a vector to be serialized.
   * @param v Vector to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startVector(ArrayList v, String tag) throws IOException;
  
  /**
   * Mark the end of a serialized vector.
   * @param v Vector to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endVector(ArrayList v, String tag) throws IOException;
  
  /**
   * Mark the start of a map to be serialized.
   * @param m Map to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void startMap(TreeMap m, String tag) throws IOException;
  
  /**
   * Mark the end of a serialized map.
   * @param m Map to be serialized
   * @param tag Used by tagged serialization formats (such as XML)
   * @throws IOException Indicates error in serialization
   */
  public void endMap(TreeMap m, String tag) throws IOException;
}
