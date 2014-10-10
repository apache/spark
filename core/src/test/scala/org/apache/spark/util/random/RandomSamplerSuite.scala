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

package org.apache.spark.util.random

import java.util.Random

import cern.jet.random.Poisson
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.mock.EasyMockSugar

class RandomSamplerSuite extends FunSuite with BeforeAndAfter with EasyMockSugar {

  val a = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

  var random: Random = _
  var poisson: Poisson = _

  before {
    random = mock[Random]
    poisson = mock[Poisson]
  }

  test("BernoulliSamplerWithRange") {
    expecting {
      for(x <- Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)) {
        random.nextDouble().andReturn(x)
      }
    }
    whenExecuting(random) {
      val sampler = new BernoulliSampler[Int](0.25, 0.55)
      sampler.rng = random
      assert(sampler.sample(a.iterator).toList == List(3, 4, 5))
    }
  }

  test("BernoulliSamplerWithRangeInverse") {
    expecting {
      for(x <- Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)) {
        random.nextDouble().andReturn(x)
      }
    }
    whenExecuting(random) {
      val sampler = new BernoulliSampler[Int](0.25, 0.55, true)
      sampler.rng = random
      assert(sampler.sample(a.iterator).toList === List(1, 2, 6, 7, 8, 9))
    }
  }

  test("BernoulliSamplerWithRatio") {
    expecting {
      for(x <- Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)) {
        random.nextDouble().andReturn(x)
      }
    }
    whenExecuting(random) {
      val sampler = new BernoulliSampler[Int](0.35)
      sampler.rng = random
      assert(sampler.sample(a.iterator).toList == List(1, 2, 3))
    }
  }

  test("BernoulliSamplerWithComplement") {
    expecting {
      for(x <- Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)) {
        random.nextDouble().andReturn(x)
      }
    }
    whenExecuting(random) {
      val sampler = new BernoulliSampler[Int](0.25, 0.55, true)
      sampler.rng = random
      assert(sampler.sample(a.iterator).toList == List(1, 2, 6, 7, 8, 9))
    }
  }

  test("BernoulliSamplerSetSeed") {
    expecting {
      random.setSeed(10L)
    }
    whenExecuting(random) {
      val sampler = new BernoulliSampler[Int](0.2)
      sampler.rng = random
      sampler.setSeed(10L)
    }
  }

  test("PoissonSampler") {
    expecting {
      for(x <- Seq(0, 1, 2, 0, 1, 1, 0, 0, 0)) {
        poisson.nextInt().andReturn(x)
      }
    }
    whenExecuting(poisson) {
      val sampler = new PoissonSampler[Int](0.2)
      sampler.rng = poisson
      assert(sampler.sample(a.iterator).toList == List(2, 3, 3, 5, 6))
    }
  }
}
