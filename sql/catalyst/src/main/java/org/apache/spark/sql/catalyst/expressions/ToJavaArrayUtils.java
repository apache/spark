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

import scala.reflect.ClassTag$;

import org.apache.spark.sql.catalyst.util.ArrayData;

import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.ByteType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.ShortType;

public class ToJavaArrayUtils {

  // boolean
  // boolean non-nullable
  public static boolean[] toBooleanArray(ArrayData arrayData) {
    return arrayData.toBooleanArray();
  }

  // Boolean nullable
  public static Boolean[] toBoxedBooleanArray(ArrayData arrayData) {
    return (Boolean[]) arrayData.toArray(BooleanType,
        ClassTag$.MODULE$.apply(java.lang.Boolean.class));
  }

  // byte
  // byte non-nullable
  public static byte[] toByteArray(ArrayData arrayData) {
    return arrayData.toByteArray();
  }

  // Byte nullable
  public static Byte[] toBoxedByteArray(ArrayData arrayData) {
    return (Byte[]) arrayData.toArray(ByteType, ClassTag$.MODULE$.apply(java.lang.Byte.class));
  }

  // short
  // short non-nullable
  public static short[] toShortArray(ArrayData arrayData) {
    return arrayData.toShortArray();
  }

  // Short nullable
  public static Short[] toBoxedShortArray(ArrayData arrayData) {
    return (Short[]) arrayData.toArray(ShortType, ClassTag$.MODULE$.apply(java.lang.Short.class));
  }

  // int
  // int non-nullable
  public static int[] toIntegerArray(ArrayData arrayData) {
    return arrayData.toIntArray();
  }

  // Integer nullable
  public static Integer[] toBoxedIntegerArray(ArrayData arrayData) {
    return (Integer[]) arrayData.toArray(IntegerType,
        ClassTag$.MODULE$.apply(java.lang.Integer.class));
  }

  // long
  // long non-nullable
  public static long[] toLongArray(ArrayData arrayData) {
    return arrayData.toLongArray();
  }

  // Long nullable
  public static Long[] toBoxedLongArray(ArrayData arrayData) {
    return (Long[]) arrayData.toArray(LongType, ClassTag$.MODULE$.apply(java.lang.Long.class));
  }

  // float
  // float non-nullable
  public static float[] toFloatArray(ArrayData arrayData) {
    return arrayData.toFloatArray();
  }

  // Float nullable
  public static Float[] toBoxedFloatArray(ArrayData arrayData) {
    return (Float[]) arrayData.toArray(FloatType, ClassTag$.MODULE$.apply(java.lang.Float.class));
  }

  // double
  // double non-nullable
  public static double[] toDoubleArray(ArrayData arrayData) {
    return arrayData.toDoubleArray();
  }

  // Double nullable
  public static Double[] toBoxedDoubleArray(ArrayData arrayData) {
    return (Double[]) arrayData.toArray(DoubleType,
        ClassTag$.MODULE$.apply(java.lang.Double.class));
  }
}
