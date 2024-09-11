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
  // Boolean nullable comparator
  private static final Comparator<Boolean> booleanComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    boolean c1 = o1, c2 = o2;
    return c1 == c2 ? 0 : (c1 ? 1 : -1);
  };

  // Byte nullable comparator
  private static final Comparator<Byte> byteComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    byte c1 = o1, c2 = o2;
    return Byte.compare(c1, c2);
  };

  // Short nullable comparator
  private static final Comparator<Short> shortComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    short c1 = o1, c2 = o2;
    return Short.compare(c1, c2);
  };

  // Integer nullable comparator
  private static final Comparator<Integer> integerComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    int c1 = o1, c2 = o2;
    return Integer.compare(c1, c2);
  };

  // Long nullable comparator
  private static final Comparator<Long> longComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    long c1 = o1, c2 = o2;
    return Long.compare(c1, c2);
  };

  // Float nullable comparator
  private static final Comparator<Float> floatComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    float c1 = o1, c2 = o2;
    return SQLOrderingUtil.compareFloats(c1, c2);
  };

  // Double nullable comparator
  private static final Comparator<Double> doubleComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    double c1 = o1, c2 = o2;
    return SQLOrderingUtil.compareDoubles(c1, c2);
  };

  // Boolean nullable comparator
  public static int binarySearch(Boolean[] data, Boolean value) {
    return Arrays.binarySearch(data, value, booleanComp);
  }

  // byte
  // byte non-nullable
  public static int binarySearch(byte[] data, byte value) {
    return Arrays.binarySearch(data, value);
  }

  // Byte nullable
  public static int binarySearch(Byte[] data, Byte value) {
    return Arrays.binarySearch(data, value, byteComp);
  }

  // short
  // short non-nullable
  public static int binarySearch(short[] data, short value) {
    return Arrays.binarySearch(data, value);
  }

  // Short nullable
  public static int binarySearch(Short[] data, Short value) {
    return Arrays.binarySearch(data, value, shortComp);
  }

  // int
  // int non-nullable
  public static int binarySearch(int[] data, int value) {
    return Arrays.binarySearch(data, value);
  }

  // Integer nullable
  public static int binarySearch(Integer[] data, Integer value) {
    return Arrays.binarySearch(data, value, integerComp);
  }

  // long
  // long non-nullable
  public static int binarySearch(long[] data, long value) {
    return Arrays.binarySearch(data, value);
  }

  // Long nullable
  public static int binarySearch(Long[] data, Long value) {
    return Arrays.binarySearch(data, value, longComp);
  }

  // float
  // float non-nullable
  public static int binarySearch(float[] data, float value) {
    return Arrays.binarySearch(data, value);
  }

  // Float nullable
  public static int binarySearch(Float[] data, Float value) {
    return Arrays.binarySearch(data, value, floatComp);
  }

  // double
  // double non-nullable
  public static int binarySearch(double[] data, double value) {
    return Arrays.binarySearch(data, value);
  }

  // Double nullable
  public static int binarySearch(Double[] data, Double value) {
    return Arrays.binarySearch(data, value, doubleComp);
  }

  // Object
  public static int binarySearch(Object[] data, Object value, Comparator<Object> comp) {
    return Arrays.binarySearch(data, value, comp);
  }
}
