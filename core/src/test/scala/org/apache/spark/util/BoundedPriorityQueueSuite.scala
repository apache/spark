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

package org.apache.spark.util

import org.apache.spark.SparkFunSuite

class BoundedPriorityQueueSuite extends SparkFunSuite {
  test("BoundedPriorityQueue poll test") {
    val pq = new BoundedPriorityQueue[Double](4)

    pq += 0.1
    pq += 1.5
    pq += 1.0
    pq += 0.3
    pq += 0.01

    assert(pq.nonEmpty)
    assert(pq.poll() == 0.1)
    assert(pq.poll() == 0.3)
    assert(pq.poll() == 1.0)
    assert(pq.poll() == 1.5)
    assert(pq.isEmpty)

    val pq2 = new BoundedPriorityQueue[(Int, Double)](4)(Ordering.by(_._2))
    pq2 += 1 -> 0.5
    pq2 += 5 -> 0.1
    pq2 += 3 -> 0.3
    pq2 += 4 -> 0.2
    pq2 += 1 -> 0.4

    assert(pq2.poll()._2 == 0.2)
    assert(pq2.poll()._2 == 0.3)
    assert(pq2.poll()._2 == 0.4)
    assert(pq2.poll()._2 == 0.5)
  }
}
