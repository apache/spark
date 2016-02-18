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

package org.apache.spark.sql.execution.local

import org.apache.spark.util.random.{BernoulliCellSampler, PoissonSampler}


class SampleNodeSuite extends LocalNodeTest {

  private def testSample(withReplacement: Boolean): Unit = {
    val seed = 0L
    val lowerb = 0.0
    val upperb = 0.3
    val maybeOut = if (withReplacement) "" else "out"
    test(s"with$maybeOut replacement") {
      val inputData = (1 to 1000).map { i => (i, i) }.toArray
      val inputNode = new DummyNode(kvIntAttributes, inputData)
      val sampleNode = new SampleNode(conf, lowerb, upperb, withReplacement, seed, inputNode)
      val sampler =
        if (withReplacement) {
          new PoissonSampler[(Int, Int)](upperb - lowerb, useGapSamplingIfPossible = false)
        } else {
          new BernoulliCellSampler[(Int, Int)](lowerb, upperb)
        }
      sampler.setSeed(seed)
      val expectedOutput = sampler.sample(inputData.iterator).toArray
      val actualOutput = sampleNode.collect().map { case row =>
        (row.getInt(0), row.getInt(1))
      }
      assert(actualOutput === expectedOutput)
    }
  }

  testSample(withReplacement = true)
  testSample(withReplacement = false)
}
