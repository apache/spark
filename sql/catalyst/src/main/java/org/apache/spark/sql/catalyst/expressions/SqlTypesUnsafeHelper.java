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

package org.apache.spark.sql.catalyst.expressions;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.UnsafeHelper;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class SqlTypesUnsafeHelper {
  private SqlTypesUnsafeHelper() {}

  public static Decimal getDecimalExceedingLong(
      byte[] bytes,
      int precision,
      int scale,
      boolean wrapWithScalaBigDecimal) {
    final BigInteger bigInteger = new BigInteger(bytes);
    BigDecimal decimal = new BigDecimal(bigInteger, scale);
    if (wrapWithScalaBigDecimal) {
      return Decimal.apply(new scala.math.BigDecimal(decimal), precision, scale);
    } else {
      return Decimal.apply(decimal, precision, scale);
    }
  }

  public static UnsafeRow getStruct(
      long offsetAndSize,
      Object baseObject,
      long baseOffset,
      int numFields) {
    final int offset = UnsafeHelper.getOffsetFromOffsetAndSize(offsetAndSize);
    final int size = UnsafeHelper.getSizeFromOffsetAndSize(offsetAndSize);
    final UnsafeRow row = new UnsafeRow(numFields);
    row.pointTo(baseObject, baseOffset + offset, size);
    return row;
  }

  public static UnsafeArrayData getArray(long offsetAndSize, Object baseObject, long baseOffset) {
    final int offset = UnsafeHelper.getOffsetFromOffsetAndSize(offsetAndSize);
    final int size = UnsafeHelper.getSizeFromOffsetAndSize(offsetAndSize);
    final UnsafeArrayData array = new UnsafeArrayData();
    array.pointTo(baseObject, baseOffset + offset, size);
    return array;
  }

  public static UnsafeMapData getMap(long offsetAndSize, Object baseObject, long baseOffset) {
    final int offset = UnsafeHelper.getOffsetFromOffsetAndSize(offsetAndSize);
    final int size = UnsafeHelper.getSizeFromOffsetAndSize(offsetAndSize);
    final UnsafeMapData map = new UnsafeMapData();
    map.pointTo(baseObject, baseOffset + offset, size);
    return map;
  }
}
