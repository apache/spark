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

package org.apache.spark.mllib.evolution

/**
 * Operations need to implement to be able to do evolutionary process.
 * @tparam I Individual Type
 * @tparam T train set type
 */
trait IndividualOperations[I, T] {

  /**
   * Take two individuals and create list of mutations
   * of these individuals. It's list because two individuals
   * can produce multiple offsprings by doing crossover differently
   * [[http://en.wikipedia.org/wiki/Crossover_(genetic_algorithm)]]
   * @return crossover function
   */
  def crossover: (I, I) => List[I]

  /**
   * Tells the error on this individual on this input.
   * [[http://en.wikipedia.org/wiki/Fitness_function]]
   * @return fitness function
   */
  def fitnessFunction: (I, T) => Double

  /**
   * Creating initial population
   * @return initial population function
   */
  def initialPopulationGenerator: (Long) => Iterator[I]

  /**
   * Selects individuals based on there scores from population
   * and crossover to produce next population.
   * [[http://en.wikipedia.org/wiki/Selection_(genetic_algorithm)]]
   * @return selection function
   */
  def selection: (List[(I, Double)], (I, I) => List[I]) => List[I]

}
