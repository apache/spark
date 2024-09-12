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

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.SQLOrderingUtil;

public class SortArrayExpressionUtils {

  // comparator
  // Boolean ascending nullable comparator
  private static final Comparator<Boolean> booleanAscendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    return o1.equals(o2) ? 0 : (o1 ? 1 : -1);
  };
  // Boolean descending nullable comparator
  private static final Comparator<Boolean> booleanDescendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    return o1.equals(o2) ? 0 : (o1 ? -1 : 1);
  };

  // Byte ascending nullable comparator
  private static final Comparator<Byte> byteAscendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    return Byte.compare(o1, o2);
  };
  // Byte descending nullable comparator
  private static final Comparator<Byte> byteDescendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    return Byte.compare(o2, o1);
  };

  // Short ascending nullable comparator
  private static final Comparator<Short> shortAscendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    return Short.compare(o1, o2);
  };
  // Short descending nullable comparator
  private static final Comparator<Short> shortDescendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    return Short.compare(o2, o1);
  };

  // Integer ascending nullable comparator
  private static final Comparator<Integer> integerAscendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    return Integer.compare(o1, o2);
  };
  // Integer descending nullable comparator
  private static final Comparator<Integer> integerDescendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    return Integer.compare(o2, o1);
  };

  // Long ascending nullable comparator
  private static final Comparator<Long> longAscendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    return Long.compare(o1, o2);
  };
  // Long descending nullable comparator
  private static final Comparator<Long> longDescendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    return Long.compare(o2, o1);
  };

  // Float ascending nullable comparator
  private static final Comparator<Float> floatAscendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    return SQLOrderingUtil.compareFloats(o1, o2);
  };
  // Float descending nullable comparator
  private static final Comparator<Float> floatDescendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    return SQLOrderingUtil.compareFloats(o2, o1);
  };

  // Double ascending nullable comparator
  private static final Comparator<Double> doubleAscendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return -1;
    } else if (o2 == null) {
      return 1;
    }
    return SQLOrderingUtil.compareDoubles(o1, o2);
  };
  // Double descending nullable comparator
  private static final Comparator<Double> doubleDescendingComp = (o1, o2) -> {
    if (o1 == null && o2 == null) {
      return 0;
    } else if (o1 == null) {
      return 1;
    } else if (o2 == null) {
      return -1;
    }
    return SQLOrderingUtil.compareDoubles(o2, o1);
  };

  // boolean
  // boolean nullable
  public static GenericArrayData sort(Boolean[] data, boolean ascendingOrder) {
    if (ascendingOrder) {
      Arrays.sort(data, booleanAscendingComp);
    } else {
      Arrays.sort(data, booleanDescendingComp);
    }
    return new GenericArrayData(data);
  }

  // byte
  public static ArrayData sort(byte[] data) {
    Arrays.sort(data);
    return UnsafeArrayData.fromPrimitiveArray(data);
  }
  public static ArrayData sort(Byte[] data, boolean ascendingOrder) {
    if (ascendingOrder) {
      Arrays.sort(data, byteAscendingComp);
    } else {
      Arrays.sort(data, byteDescendingComp);
    }
    return new GenericArrayData(data);
  }

  public static ArrayData sort(short[] data) {
    Arrays.sort(data);
    return UnsafeArrayData.fromPrimitiveArray(data);
  }
  public static ArrayData sort(Short[] data, boolean ascendingOrder) {
    if (ascendingOrder) {
      Arrays.sort(data, shortAscendingComp);
    } else {
      Arrays.sort(data, shortDescendingComp);
    }
    return new GenericArrayData(data);
  }

  public static ArrayData sort(int[] data) {
    Arrays.sort(data);
    return UnsafeArrayData.fromPrimitiveArray(data);
  }
  public static ArrayData sort(Integer[] data, boolean ascendingOrder) {
    if (ascendingOrder) {
      Arrays.sort(data, integerAscendingComp);
    } else {
      Arrays.sort(data, integerDescendingComp);
    }
    return new GenericArrayData(data);
  }

  public static ArrayData sort(long[] data) {
    Arrays.sort(data);
    return UnsafeArrayData.fromPrimitiveArray(data);
  }
  public static ArrayData sort(Long[] data, boolean ascendingOrder) {
    if (ascendingOrder) {
      Arrays.sort(data, longAscendingComp);
    } else {
      Arrays.sort(data, longDescendingComp);
    }
    return new GenericArrayData(data);
  }

  public static ArrayData sort(float[] data) {
    Arrays.sort(data);
    return UnsafeArrayData.fromPrimitiveArray(data);
  }
  public static ArrayData sort(Float[] data, boolean ascendingOrder) {
    if (ascendingOrder) {
      Arrays.sort(data, floatAscendingComp);
    } else {
      Arrays.sort(data, floatDescendingComp);
    }
    return new GenericArrayData(data);
  }

  public static ArrayData sort(double[] data) {
    Arrays.sort(data);
    return UnsafeArrayData.fromPrimitiveArray(data);
  }
  public static ArrayData sort(Double[] data, boolean ascendingOrder) {
    if (ascendingOrder) {
      Arrays.sort(data, doubleAscendingComp);
    } else {
      Arrays.sort(data, doubleDescendingComp);
    }
    return new GenericArrayData(data);
  }

  public static ArrayData sort(Object[] data, Comparator<Object> comp) {
    Arrays.sort(data, comp);
    return new GenericArrayData(data);
  }
}
