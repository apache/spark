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

package org.apache.spark.api.java;

import java.util.ArrayList;
import java.util.List;

// See
// http://scala-programming-language.1934581.n4.nabble.com/Workaround-for-implementing-java-varargs-in-2-7-2-final-tp1944767p1944772.html
abstract class JavaSparkContextVarargsWorkaround {

  @SafeVarargs
  public final <T> JavaRDD<T> union(JavaRDD<T>... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    List<JavaRDD<T>> rest = new ArrayList<>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  public JavaDoubleRDD union(JavaDoubleRDD... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    List<JavaDoubleRDD> rest = new ArrayList<>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  @SafeVarargs
  public final <K, V> JavaPairRDD<K, V> union(JavaPairRDD<K, V>... rdds) {
    if (rdds.length == 0) {
      throw new IllegalArgumentException("Union called on empty list");
    }
    List<JavaPairRDD<K, V>> rest = new ArrayList<>(rdds.length - 1);
    for (int i = 1; i < rdds.length; i++) {
      rest.add(rdds[i]);
    }
    return union(rdds[0], rest);
  }

  // These methods take separate "first" and "rest" elements to avoid having the same type erasure
  public abstract <T> JavaRDD<T> union(JavaRDD<T> first, List<JavaRDD<T>> rest);
  public abstract JavaDoubleRDD union(JavaDoubleRDD first, List<JavaDoubleRDD> rest);
  public abstract <K, V> JavaPairRDD<K, V> union(JavaPairRDD<K, V> first, List<JavaPairRDD<K, V>> rest);
}
