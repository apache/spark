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

import org.apache.spark.sql.catalyst.util.STUtils;

/**
 * Interface for converting a WKB byte array into a physical geometry or geography value.
 */
interface WKBConverterStrategy {
  /**
   * Converts the WKB bytes in the sub-range {@code [offset, offset + length)} of {@code wkb}
   * into a physical geometry/geography value. Accepting an offset/length lets callers reuse a
   * larger backing buffer without materializing an exact-size copy.
   */
  byte[] convert(byte[] wkb, int offset, int length, int srid);

  /** Convenience overload that converts the entire {@code wkb} array. */
  default byte[] convert(byte[] wkb, int srid) {
    return convert(wkb, 0, wkb.length, srid);
  }
}

/**
 * Converts the provided WKB data into a geometry object.
 */
enum WKBToGeometryConverter implements WKBConverterStrategy {
  INSTANCE;

  @Override
  public byte[] convert(byte[] wkb, int offset, int length, int srid) {
    return STUtils.stGeomFromWKB(wkb, offset, length, srid).getBytes();
  }
}

/**
 * Converts the provided WKB data into a geography object.
 */
enum WKBToGeographyConverter implements WKBConverterStrategy {
  INSTANCE;

  @Override
  public byte[] convert(byte[] wkb, int offset, int length, int srid) {
    return STUtils.stGeogFromWKB(wkb, offset, length, srid).getBytes();
  }
}
