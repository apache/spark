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

import java.util.Arrays;
import java.util.Comparator;

import org.apache.spark.sql.catalyst.util.SQLOrderingUtil;

public class ArrayExpressionUtils {

  // comparator
  // boolean nullable comparator
  private static final Comparator<Object> booleanComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    boolean c1 = (Boolean) o1, c2 = (Boolean) o2;
    return c1 == c2 ? 0 : (c1 ? 1 : -1);
  };

  // byte nullable comparator
  private static final Comparator<Object> byteComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    byte c1 = (Byte) o1, c2 = (Byte) o2;
    return Byte.compare(c1, c2);
  };

  // short nullable comparator
  private static final Comparator<Object> shortComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    short c1 = (Short) o1, c2 = (Short) o2;
    return Short.compare(c1, c2);
  };

  // integer nullable comparator
  private static final Comparator<Object> integerComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    int c1 = (Integer) o1, c2 = (Integer) o2;
    return Integer.compare(c1, c2);
  };

  // long nullable comparator
  private static final Comparator<Object> longComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    long c1 = (Long) o1, c2 = (Long) o2;
    return Long.compare(c1, c2);
  };

  // float nullable comparator
  private static final Comparator<Object> floatComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    float c1 = (Float) o1, c2 = (Float) o2;
    return SQLOrderingUtil.compareFloats(c1, c2);
  };

  // double nullable comparator
  private static final Comparator<Object> doubleComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    double c1 = (Double) o1, c2 = (Double) o2;
    return SQLOrderingUtil.compareDoubles(c1, c2);
  };

  // boolean nullable comparator
  public static int binarySearch(Boolean[] data, Boolean value) {
    return Arrays.binarySearch(data, value, booleanComp);
  }

  // byte
  // byte non-nullable
  public static int binarySearch(byte[] data, byte value) {
    return Arrays.binarySearch(data, value);
  }

  // byte nullable
  public static int binarySearch(Byte[] data, Byte value) {
    return Arrays.binarySearch(data, value, byteComp);
  }

  // short
  // short non-nullable
  public static int binarySearch(short[] data, short value) {
    return Arrays.binarySearch(data, value);
  }

  // short nullable
  public static int binarySearch(Short[] data, Short value) {
    return Arrays.binarySearch(data, value, shortComp);
  }

  // int
  // int non-nullable
  public static int binarySearch(int[] data, int value) {
    return Arrays.binarySearch(data, value);
  }

  // int nullable
  public static int binarySearch(Integer[] data, Integer value) {
    return Arrays.binarySearch(data, value, integerComp);
  }

  // long
  // long non-nullable
  public static int binarySearch(long[] data, long value) {
    return Arrays.binarySearch(data, value);
  }

  // long nullable
  public static int binarySearch(Long[] data, Long value) {
    return Arrays.binarySearch(data, value, longComp);
  }

  // float
  // float non-nullable
  public static int binarySearch(float[] data, float value) {
    return Arrays.binarySearch(data, value);
  }

  // float nullable
  public static int binarySearch(Float[] data, Float value) {
    return Arrays.binarySearch(data, value, floatComp);
  }

  // double
  // double non-nullable
  public static int binarySearch(double[] data, double value) {
    return Arrays.binarySearch(data, value);
  }

  // double nullable
  public static int binarySearch(Double[] data, Double value) {
    return Arrays.binarySearch(data, value, doubleComp);
  }

  // object
  public static int binarySearch(Object[] data, Object value, Comparator<Object> comp) {
    return Arrays.binarySearch(data, value, comp);
  }
}
