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

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An implementation of the Parquet DELTA_BYTE_ARRAY decoder that supports the vectorized interface.
 */
public class VectorizedDeltaByteArrayReader extends VectorizedReaderBase {
  private final DeltaByteArrayReader deltaByteArrayReader = new DeltaByteArrayReader();

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    deltaByteArrayReader.initFromPage(valueCount, in);
  }

  @Override
  public Binary readBinary(int len) {
    return deltaByteArrayReader.readBytes();
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    for (int i = 0; i < total; i++) {
      Binary binary = deltaByteArrayReader.readBytes();
      ByteBuffer buffer = binary.toByteBuffer();
      if (buffer.hasArray()) {
        c.putByteArray(rowId + i, buffer.array(), buffer.arrayOffset() + buffer.position(),
          binary.length());
      } else {
        byte[] bytes = new byte[binary.length()];
        buffer.get(bytes);
        c.putByteArray(rowId + i, bytes);
      }
    }
  }

  @Override
  public void skipBinary(int total) {
    for (int i = 0; i < total; i++) {
      deltaByteArrayReader.skip();
    }
  }

}
