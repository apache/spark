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
import cern.jet.random.engine.DRand

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * A pseudorandom sampler. It is possible to change the sampled item type. For example, we might
 * want to add weights for stratified sampling or importance sampling. Should only use
 * transformations that are tied to the sampler and cannot be applied after sampling.
 *
 * @tparam T item type
 * @tparam U sampled item type
 */
@DeveloperApi
trait RandomSampler[T, U] extends Pseudorandom with Cloneable with Serializable {

  /** take a random sample */
  def sample(items: Iterator[T]): Iterator[U]

  override def clone: RandomSampler[T, U] =
    throw new NotImplementedError("clone() is not implemented.")
}

/**
 * :: DeveloperApi ::
 * A sampler based on Bernoulli trials.
 *
 * @param lb lower bound of the acceptance range
 * @param ub upper bound of the acceptance range
 * @param complement whether to use the complement of the range specified, default to false
 * @tparam T item type
 */
@DeveloperApi
class BernoulliSampler[T](lb: Double, ub: Double, complement: Boolean = false)
    (implicit random: Random = new XORShiftRandom)
  extends RandomSampler[T, T] {

  def this(ratio: Double)(implicit random: Random = new XORShiftRandom)
    = this(0.0d, ratio)(random)

  override def setSeed(seed: Long) = random.setSeed(seed)

  override def sample(items: Iterator[T]): Iterator[T] = {
    items.filter { item =>
      val x = random.nextDouble()
      (x >= lb && x < ub) ^ complement
    }
  }

  /**
   *  Return a sampler that is the complement of the range specified of the current sampler.
   */
  def cloneComplement():  BernoulliSampler[T] = new BernoulliSampler[T](lb, ub, !complement)

  override def clone = new BernoulliSampler[T](lb, ub, complement)
}

/**
 * :: DeveloperApi ::
 * A sampler based on values drawn from Poisson distribution.
 *
 * @param poisson a Poisson random number generator
 * @tparam T item type
 */
@DeveloperApi
class PoissonSampler[T](mean: Double)
    (implicit var poisson: Poisson = new Poisson(mean, new DRand))
  extends RandomSampler[T, T] {

  override def setSeed(seed: Long) {
    poisson = new Poisson(mean, new DRand(seed.toInt))
  }

  override def sample(items: Iterator[T]): Iterator[T] = {
    items.flatMap { item =>
      val count = poisson.nextInt()
      if (count == 0) {
        Iterator.empty
      } else {
        Iterator.fill(count)(item)
      }
    }
  }

  override def clone = new PoissonSampler[T](mean)
}
