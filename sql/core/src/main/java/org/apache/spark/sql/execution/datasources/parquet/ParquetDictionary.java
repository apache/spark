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

import org.apache.spark.sql.execution.vectorized.Dictionary;

import java.math.BigInteger;

public final class ParquetDictionary implements Dictionary {
  private org.apache.parquet.column.Dictionary dictionary;
  private boolean needTransform = false;

  public ParquetDictionary(org.apache.parquet.column.Dictionary dictionary, boolean needTransform) {
    this.dictionary = dictionary;
    this.needTransform = needTransform;
  }

  @Override
  public int decodeToInt(int id) {
    if (needTransform) {
      return (int) dictionary.decodeToLong(id);
    } else {
      return dictionary.decodeToInt(id);
    }
  }

  @Override
  public long decodeToLong(int id) {
    if (needTransform) {
      // For unsigned int32, it stores as dictionary encoded signed int32 in Parquet
      // whenever dictionary is available.
      // Here we lazily decode it to the original signed int value then convert to long(unit32).
      return Integer.toUnsignedLong(dictionary.decodeToInt(id));
    } else {
      return dictionary.decodeToLong(id);
    }
  }

  @Override
  public float decodeToFloat(int id) {
    return dictionary.decodeToFloat(id);
  }

  @Override
  public double decodeToDouble(int id) {
    return dictionary.decodeToDouble(id);
  }

  @Override
  public byte[] decodeToBinary(int id) {
    if (needTransform) {
      // For unsigned int64, it stores as dictionary encoded signed int64 in Parquet
      // whenever dictionary is available.
      // Here we lazily decode it to the original signed long value then convert to decimal(20, 0).
      long signed = dictionary.decodeToLong(id);
      return new BigInteger(Long.toUnsignedString(signed)).toByteArray();
    } else {
      return dictionary.decodeToBinary(id).getBytesUnsafe();
    }
  }
}
