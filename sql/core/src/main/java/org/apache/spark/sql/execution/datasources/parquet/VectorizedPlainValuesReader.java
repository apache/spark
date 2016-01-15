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

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.unsafe.Platform;

import org.apache.parquet.column.values.ValuesReader;

/**
 * An implementation of the Parquet PLAIN decoder that supports the vectorized interface.
 */
public class VectorizedPlainValuesReader extends ValuesReader implements VectorizedValuesReader {
  private byte[] buffer;
  private int offset;
  private final int byteSize;

  public VectorizedPlainValuesReader(int byteSize) {
    this.byteSize = byteSize;
  }

  @Override
  public void initFromPage(int valueCount, byte[] bytes, int offset) throws IOException {
    this.buffer = bytes;
    this.offset = offset + Platform.BYTE_ARRAY_OFFSET;
  }

  @Override
  public void skip() {
    offset += byteSize;
  }

  @Override
  public void skip(int n) {
    offset += n * byteSize;
  }

  @Override
  public void readIntegers(int total, ColumnVector c, int rowId) {
    c.putIntsLittleEndian(rowId, total, buffer, offset - Platform.BYTE_ARRAY_OFFSET);
    offset += 4 * total;
  }

  @Override
  public int readInteger() {
    int v = Platform.getInt(buffer, offset);
    offset += 4;
    return v;
  }
}
