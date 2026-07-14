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

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.execution.vectorized.Dictionary;

public final class ParquetDictionary implements Dictionary {
  private org.apache.parquet.column.Dictionary dictionary;
  private boolean needTransform = false;

  // TIME-specific lazy dictionary decode fields: micros->nanos + truncation.
  private boolean isTimeTransform = false;
  private boolean fileStoresNanos = false;
  private int timePrecision = 0;

  public ParquetDictionary(org.apache.parquet.column.Dictionary dictionary, boolean needTransform) {
    this.dictionary = dictionary;
    this.needTransform = needTransform;
  }

  /**
   * Constructs a ParquetDictionary with TIME transform support. When {@code isTimeTransform} is
   * true, {@link #decodeToLong(int)} converts the raw dictionary value from the on-disk unit
   * (micros or nanos) to internal nanos and truncates to the requested precision - the same
   * transform the eager path ({@code TimeVectorUpdater.toTruncatedNanos}) applies per value.
   */
  public ParquetDictionary(
      org.apache.parquet.column.Dictionary dictionary,
      boolean needTransform,
      boolean isTimeTransform,
      boolean fileStoresNanos,
      int timePrecision) {
    this(dictionary, needTransform);
    this.isTimeTransform = isTimeTransform;
    this.fileStoresNanos = fileStoresNanos;
    this.timePrecision = timePrecision;
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
    if (isTimeTransform) {
      long raw = dictionary.decodeToLong(id);
      long nanos = fileStoresNanos ? raw : DateTimeUtils.microsToNanos(raw);
      return DateTimeUtils.truncateTimeToPrecision(nanos, timePrecision);
    } else if (needTransform) {
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
      return VectorizedReaderBase.unsignedLongToBytesBigEndian(signed);
    } else {
      return dictionary.decodeToBinary(id).getBytesUnsafe();
    }
  }
}
