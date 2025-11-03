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

package org.apache.spark.rdd

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.random.{BernoulliSampler, PoissonSampler, RandomSampler}

/** a sampler that outputs its seed */
class MockSampler extends RandomSampler[Long, Long] {

  private var s: Long = _

  override def setSeed(seed: Long): Unit = {
    s = seed
  }

  override def sample(): Int = 1

  override def sample(items: Iterator[Long]): Iterator[Long] = {
    Iterator(s)
  }

  override def clone: MockSampler = new MockSampler
}

class PartitionwiseSampledRDDSuite extends SparkFunSuite with SharedSparkContext {

  test("seed distribution") {
    val rdd = sc.makeRDD(Array(1L, 2L, 3L, 4L).toImmutableArraySeq, 2)
    val sampler = new MockSampler
    val sample = new PartitionwiseSampledRDD[Long, Long](rdd, sampler, false, 0L)
    assert(sample.distinct().count() == 2, "Seeds must be different.")
  }

  test("concurrency") {
    // SPARK-2251: zip with self computes each partition twice.
    // We want to make sure there are no concurrency issues.
    val rdd = sc.parallelize(0 until 111, 10)
    for (sampler <- Seq(new BernoulliSampler[Int](0.5), new PoissonSampler[Int](0.5))) {
      val sampled = new PartitionwiseSampledRDD[Int, Int](rdd, sampler, true)
      sampled.zip(sampled).count()
    }
  }
}

