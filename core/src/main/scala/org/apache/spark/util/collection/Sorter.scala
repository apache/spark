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

package org.apache.spark.util.collection

import java.util.Comparator

/**
 * A simple wrapper over the Java implementation [[TimSort]].
 *
 * The Java implementation is package private, and hence it cannot be called outside package
 * org.apache.spark.util.collection. This is a simple wrapper of it that is available to spark.
 */
private[spark]
class Sorter[K, Buffer](private val s: SortDataFormat[K, Buffer]) {

  private val timSort = new TimSort(s)

  /**
   * Sorts the input buffer within range [lo, hi).
   */
  def sort(a: Buffer, lo: Int, hi: Int, c: Comparator[_ >: K]): Unit = {
    timSort.sort(a, lo, hi, c)
  }
}
