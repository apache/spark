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

import java.util.Comparator;

import scala.math.Ordering;

import org.apache.spark.sql.catalyst.types.PhysicalDataType$;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.SQLOrderingUtil;
import org.apache.spark.sql.types.ByteType$;
import org.apache.spark.sql.types.BooleanType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DoubleType$;
import org.apache.spark.sql.types.FloatType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.ShortType$;

public class ArrayExpressionUtils {

  public static int binarySearchNullSafe(ArrayData data, Boolean value) {
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      boolean c1 = (Boolean) o1;
      boolean c2 = (Boolean) o2;
      return c1 == c2 ? 0 : (c1 ? 1 : -1);
    };
    return java.util.Arrays.binarySearch(data.toObjectArray(BooleanType$.MODULE$), value, comp);
  }

  public static int binarySearch(ArrayData data, byte value) {
    return java.util.Arrays.binarySearch(data.toByteArray(), value);
  }

  public static int binarySearchNullSafe(ArrayData data, Byte value) {
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      byte c1 = (Byte) o1;
      byte c2 = (Byte) o2;
      return Byte.compare(c1, c2);
    };
    return java.util.Arrays.binarySearch(data.toObjectArray(ByteType$.MODULE$), value, comp);
  }

  public static int binarySearch(ArrayData data, short value) {
    return java.util.Arrays.binarySearch(data.toShortArray(), value);
  }

  public static int binarySearchNullSafe(ArrayData data, Short value) {
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      short c1 = (Short) o1;
      short c2 = (Short) o2;
      return Short.compare(c1, c2);
    };
    return java.util.Arrays.binarySearch(data.toObjectArray(ShortType$.MODULE$), value, comp);
  }

  public static int binarySearch(ArrayData data, int value) {
    return java.util.Arrays.binarySearch(data.toIntArray(), value);
  }

  public static int binarySearchNullSafe(ArrayData data, Integer value) {
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      int c1 = (Integer) o1;
      int c2 = (Integer) o2;
      return Integer.compare(c1, c2);
    };
    return java.util.Arrays.binarySearch(data.toObjectArray(IntegerType$.MODULE$), value, comp);
  }

  public static int binarySearch(ArrayData data, long value) {
    return java.util.Arrays.binarySearch(data.toLongArray(), value);
  }

  public static int binarySearchNullSafe(ArrayData data, Long value) {
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      long c1 = (Long) o1;
      long c2 = (Long) o2;
      return Long.compare(c1, c2);
    };
    return java.util.Arrays.binarySearch(data.toObjectArray(LongType$.MODULE$), value, comp);
  }

  public static int binarySearch(ArrayData data, float value) {
    return java.util.Arrays.binarySearch(data.toFloatArray(), value);
  }

  public static int binarySearchNullSafe(ArrayData data, Float value) {
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      float c1 = (Float) o1;
      float c2 = (Float) o2;
      return SQLOrderingUtil.compareFloats(c1, c2);
    };
    return java.util.Arrays.binarySearch(data.toObjectArray(FloatType$.MODULE$), value, comp);
  }

  public static int binarySearch(ArrayData data, double value) {
    return java.util.Arrays.binarySearch(data.toDoubleArray(), value);
  }

  public static int binarySearchNullSafe(ArrayData data, Double value) {
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      double c1 = (Double) o1;
      double c2 = (Double) o2;
      return SQLOrderingUtil.compareDoubles(c1, c2);
    };
    return java.util.Arrays.binarySearch(data.toObjectArray(DoubleType$.MODULE$), value, comp);
  }

  public static int binarySearch(DataType elementType, ArrayData data, Object value) {
    Object[] array = data.toObjectArray(elementType);
    Ordering<Object> ordering = PhysicalDataType$.MODULE$.ordering(elementType);
    Comparator<Object> comp = (o1, o2) -> {
      if (o1 == null && o2 == null) {
        return 0;
      } else if (o1 == null) {
        return -1;
      } else if (o2 == null) {
        return 1;
      }
      return ordering.compare(o1, o2);
    };
    return java.util.Arrays.binarySearch(array, value, comp);
  }
}
