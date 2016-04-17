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

package org.apache.spark.util.collection.unsafe.sort

import java.util.Comparator

import org.apache.spark.unsafe.array.LongArray
import org.apache.spark.util.collection.Sorter

object UnsafeSortTestUtil {
  def sortKeyPrefixArrayByPrefix(array: LongArray, length: Int, cmp: PrefixComparator): Unit = {
    val referenceSort = new Sorter(UnsafeSortDataFormat.INSTANCE)
    referenceSort.sort(
      array, 0, length, new Comparator[RecordPointerAndKeyPrefix] {
        override def compare(
            r1: RecordPointerAndKeyPrefix,
            r2: RecordPointerAndKeyPrefix): Int = {
          cmp.compare(r1.keyPrefix, r2.keyPrefix)
        }
      })
  }
}
