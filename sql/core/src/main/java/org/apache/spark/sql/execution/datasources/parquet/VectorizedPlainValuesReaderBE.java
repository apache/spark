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
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.spark.unsafe.Platform;

/**
 * Implementation of VectorizedPlainValuesReader for Big Endian platforms
 */
public class VectorizedPlainValuesReaderBE extends VectorizedPlainValuesReader {
 
  private ByteBuffer byteBuffer; // used to wrap the byte array buffer
  
  public VectorizedPlainValuesReaderBE() {
    super();
  }

  @Override
  public void initFromPage(int valueCount, byte[] bytes, int offset) throws IOException {
    super.initFromPage(valueCount, bytes, offset);
    byteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
  }

  @Override
  public int readInteger() {
    return java.lang.Integer.reverseBytes(super.readInteger());
  }

  @Override
  public long readLong() {
    return java.lang.Long.reverseBytes(super.readLong());
  }

  @Override
  public float readFloat() {
    float v = byteBuffer.getFloat(offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 4;
    return v;
  }

  @Override
  public double readDouble() {
    double v = byteBuffer.getDouble(offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 8;
    return v;
  }

}
