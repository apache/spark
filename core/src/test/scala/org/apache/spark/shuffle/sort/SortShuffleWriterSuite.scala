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

package org.apache.spark.shuffle.sort

import org.mockito.Mockito._

import org.apache.spark.{Aggregator, SparkConf, SparkFunSuite}

class SortShuffleWriterSuite extends SparkFunSuite {

  import SortShuffleWriter._

  test("conditions for bypassing merge-sort") {
    val conf = new SparkConf(loadDefaults = false)
    val agg = mock(classOf[Aggregator[_, _, _]], RETURNS_SMART_NULLS)
    val ord = implicitly[Ordering[Int]]

    // Numbers of partitions that are above and below the default bypassMergeThreshold
    val FEW_PARTITIONS = 50
    val MANY_PARTITIONS = 10000

    // Shuffles with no ordering or aggregator: should bypass unless # of partitions is high
    assert(shouldBypassMergeSort(conf, FEW_PARTITIONS, None, None))
    assert(!shouldBypassMergeSort(conf, MANY_PARTITIONS, None, None))

    // Shuffles with an ordering or aggregator: should not bypass even if they have few partitions
    assert(!shouldBypassMergeSort(conf, FEW_PARTITIONS, None, Some(ord)))
    assert(!shouldBypassMergeSort(conf, FEW_PARTITIONS, Some(agg), None))
  }
}
