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

package org.apache.spark.sql.catalyst.expressions.codegen;

import java.nio.charset.StandardCharsets;

import org.apache.spark.unsafe.types.UTF8String;

/**
 * A helper class to write `UTF8String`, `String`, and `byte[]` data into an internal buffer
 * and get a final concatenated string.
 */
public class UTF8StringBuilder {

  private StringBuilder buffer;

  public UTF8StringBuilder() {
    this.buffer = new StringBuilder();
  }

  public void append(UTF8String value) {
    buffer.append(value);
  }

  public void append(String value) {
    buffer.append(value);
  }

  public void append(byte[] value) {
    buffer.append(new String(value, StandardCharsets.UTF_8));
  }

  @Override
  public String toString() {
    return buffer.toString();
  }
}
