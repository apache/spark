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
package org.apache.spark.streaming.kinesis

import org.apache.spark.Logging

/**
 * Implementation of KinesisRecordSerializer to convert Array[Byte] to/from String.
 */
class KinesisStringRecordSerializer extends KinesisRecordSerializer[String] with Logging {
  /**
   * Convert String to Array[Byte]
   *
   * @param string to serialize
   * @return byte array
   */
  def serialize(string: String): Array[Byte] = {
    string.getBytes()
  }

  /**
   * Convert Array[Byte] to String
   *
   * @param byte array
   * @return deserialized string
   */
  def deserialize(array: Array[Byte]): String = {
    new String(array)
  }
}
