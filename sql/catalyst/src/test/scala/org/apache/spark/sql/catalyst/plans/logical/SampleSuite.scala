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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkFunSuite

class SampleSuite extends SparkFunSuite {

  test("resolveSeed returns a user-specified seed unchanged") {
    assert(Sample.resolveSeed(Some(42L)) === 42L)
    assert(Sample.resolveSeed(Some(0L)) === 0L)
    assert(Sample.resolveSeed(Some(Long.MaxValue)) === Long.MaxValue)
    // Only generated seeds are constrained to be non-negative. The Dataset API accepts a
    // negative seed even though the SQL REPEATABLE grammar does not, so it must pass through.
    assert(Sample.resolveSeed(Some(-5L)) === -5L)
    assert(Sample.resolveSeed(Some(Long.MinValue)) === Long.MinValue)
  }

  test("resolveSeed generates non-negative seeds") {
    // A pushed-down sample renders its seed into SQL as `REPEATABLE (<seed>)`, and the seed
    // in that grammar does not accept a sign.
    for (_ <- 0 until 10000) {
      assert(Sample.resolveSeed(None) >= 0L)
    }
  }

  test("resolveSeed draws from a wide range of values") {
    // Guards against SPARK-56573, where the generated seed was limited to 1000 distinct
    // values. Drawing from 2^63 makes 1000 collisions in 10000 draws effectively impossible.
    val seeds = Seq.fill(10000)(Sample.resolveSeed(None)).toSet
    assert(seeds.size > 9000, s"expected nearly all seeds to be distinct, got ${seeds.size}")
    // The old implementation could never exceed 999.
    assert(seeds.exists(_ > 1000L))
  }
}
