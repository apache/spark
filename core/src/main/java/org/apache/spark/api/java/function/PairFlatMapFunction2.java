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

package org.apache.spark.api.java.function;

import scala.Tuple2;

import java.io.Serializable;

/**
 * A function that takes arguments of type T1 and T2, and returns zero or more key-value pair
 * records from each input record. The key-value pairs are represented as scala.Tuple2 objects.
 */
public interface PairFlatMapFunction2<T1, T2 , K, V> extends Serializable {
  public Iterable<Tuple2<K, V>> call(T1 t1, T2 t2) throws Exception;
}
