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

package org.apache.spark.util;

/**
 * Provides a wrapper around an object that is known to be an array, but the specific
 * type for the array is unknown.
 *
 * Normally, in situations when such an array is to be accessed reflectively, one would use
 * {@link java.lang.reflect.Array} using getLength() and get() methods. However, it turns
 * out that such methods are ill-performant.
 *
 * It turns out it is better to just use instanceOf and lots of casting over calling through
 * to the native C implementation. There is some discussion and a sample code snippet in
 * <a href=https://bugs.openjdk.java.net/browse/JDK-8051447>an open JDK ticket</a>. In this
 * class, that approach is implemented in an alternative way: creating a wrapper object to
 * wrap the array allows the cast to be done once, so the overhead of casting multiple times
 * is also avoided. It turns out we invoke the get() method to get the value of the array
 * numerous times, so doing the cast just once is worth the cost of constructing the wrapper
 * object for larger arrays.
 */
public abstract class ReflectArrayGetter {

  public static ReflectArrayGetter create(Object array) {
    if (array instanceof Object[]) {
      Object[] casted = (Object[]) array;
      return new ObjectArrayGetter(casted);
    } else if (array instanceof boolean[]) {
      boolean[] casted = (boolean[]) array;
      return new BooleanArrayGetter(casted);
    } else if (array instanceof int[]) {
      int[] casted = (int[]) array;
      return new IntArrayGetter(casted);
    } else if (array instanceof byte[]) {
      byte[] casted = (byte[]) array;
      return new ByteArrayGetter(casted);
    } else if (array instanceof short[]) {
      short[] casted = (short[]) array;
      return new ShortArrayGetter(casted);
    } else if (array instanceof char[]) {
      char[] casted = (char[]) array;
      return new CharArrayGetter(casted);
    } else if (array instanceof long[]) {
      long[] casted = (long[]) array;
      return new LongArrayGetter(casted);
    } else if (array instanceof double[]) {
      double[] casted = (double[]) array;
      return new DoubleArrayGetter(casted);
    } else if (array instanceof float[]) {
      float[] casted = (float[]) array;
      return new FloatArrayGetter(casted);
    } else {
      throw badArray(array);
    }
  }

  public abstract int getLength();

  public abstract Object get(Integer i);

  private static RuntimeException badArray(Object array) {
    if (array == null) {
      return new NullPointerException("Array argument is null");
    } else if (!array.getClass().isArray()) {
      return new IllegalArgumentException("Argument is not an array");
    } else {
      return new IllegalArgumentException("Array is of incompatible type");
    }
  }

  private static class BooleanArrayGetter extends ReflectArrayGetter {
    private boolean[] arr;

    public BooleanArrayGetter(boolean[] arr) {
      this.arr = arr;
    }

    @Override
    public Boolean get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class ByteArrayGetter extends ReflectArrayGetter {
    private byte[] arr;

    public ByteArrayGetter(byte[] arr) {
      this.arr = arr;
    }

    @Override
    public Byte get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class CharArrayGetter extends ReflectArrayGetter {
    private char[] arr;

    public CharArrayGetter(char[] arr) {
      this.arr = arr;
    }

    @Override
    public Character get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class IntArrayGetter extends ReflectArrayGetter {
    private int[] arr;

    public IntArrayGetter(int[] arr) {
      this.arr = arr;
    }

    @Override
    public Integer get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class ShortArrayGetter extends ReflectArrayGetter {
    private short[] arr;

    public ShortArrayGetter(short[] arr) {
      this.arr = arr;
    }

    @Override
    public Short get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class LongArrayGetter extends ReflectArrayGetter {
    private long[] arr;

    public LongArrayGetter(long[] arr) {
      this.arr = arr;
    }

    @Override
    public Long get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class FloatArrayGetter extends ReflectArrayGetter {
    private float[] arr;

    public FloatArrayGetter(float[] arr) {
      this.arr = arr;
    }

    @Override
    public Float get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class DoubleArrayGetter extends ReflectArrayGetter {
    private double[] arr;

    public DoubleArrayGetter(double[] arr) {
      this.arr = arr;
    }

    @Override
    public Double get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }

  private static class ObjectArrayGetter extends ReflectArrayGetter {
    private Object[] arr;

    public ObjectArrayGetter(Object[] arr) {
      this.arr = arr;
    }

    @Override
    public Object get(Integer v1) {
      return arr[v1];
    }

    @Override
    public int getLength() {
      return arr.length;
    }
  }
}
