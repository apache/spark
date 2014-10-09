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

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Central class for running evolutionary processes. It contains logic about iteratively
 * applying models to train set, calculating errors, crossover and all over again.
 * Id doesn't contains logic about crossover and initial population creation - it is in
 * utils classes.
 * [[http://en.wikipedia.org/wiki/Evolutionary_computation]]
 */
object EvolutionRunner extends Logging {

  /**
   * Central method for execution evolutionary algorithm.
   * Parallelization is based on parallelization of train set.
   * @param train train set
   * @param populationSize number of individuals in population
   * @param maxIterations maximum number of iterations in execution
   * @param shouldStop strategy when to stop based on iterations number and algorithm
   *  stopping making progress
   * @param individualType information about individual to process. Combines information
   *  about crossover,fitness function, initial population generator and selection function.
   * @return best individual and it's error
   */
  def run[I, T](
           train: RDD[T],
           populationSize: Int = 100,
           maxIterations: Int = 100,
           shouldStop: (Int, Int, List[Double]) => Boolean = stopOnIterationCount,
           individualType: IndividualOperations[I, T]
           ): (I, Double) = {
    run(train,
      populationSize,
      maxIterations,
      shouldStop,
      individualType.crossover,
      individualType.fitnessFunction,
      individualType.initialPopulationGenerator,
      individualType.selection)
  }

  /**
   * More internal method. Use individual type instead.
   * @param train  train set
   * @param populationSize  number of individuals in population
   * @param maxIterations  maximum number of iterations in execution
   * @param shouldStop  strategy when to stop based on iterations
   *                    number and algorithm stopping making progress
   * @param crossover crossover function
   * @param fitnessFunction fitness function
   * @param initialPopulationGenerator initial population generator
   * @param selection selection fucntion
   * @return best individual and it's error
   */
  def run[I, T](
           train: RDD[T],
           populationSize: Int,
           maxIterations: Int,
           shouldStop: (Int, Int, List[Double]) => Boolean,
           crossover: (I, I) => List[I],
           fitnessFunction: (I, T) => Double,
           initialPopulationGenerator: Long => Iterator[I],
           selection: (List[(I, Double)], (I, I) => List[I]) => List[I]
           ): (I, Double) = {

    var currentPopulation = initalizePopulation(initialPopulationGenerator, populationSize)
    var previousPopulation: List[I] = null
    var currentScoreResult: List[Double] = null
    var iteration = 0

    do {

      currentScoreResult = train
        .map(t => {
        currentPopulation
          .map(i => fitnessFunction(i, t))
      })
        .reduce(addErrors)

      logInfo(s"Iteration number $iteration\n" +
        s"Summary error = ${currentScoreResult.sum}, min error = ${currentScoreResult.min}")

      iteration += 1
      previousPopulation = currentPopulation
      currentPopulation = selection(currentPopulation.zip(currentScoreResult), crossover)
    } while (!shouldStop(maxIterations, iteration, currentScoreResult))

    previousPopulation
      .zip(currentScoreResult)
      .minBy(_._2)

  }

  def initalizePopulation[I](individualGenerator: Long => Iterator[I], populationSize: Int) = {
    val initialPopulationIterator = individualGenerator(Random.nextInt())
    val initialPopulationBuffer = new ListBuffer[I]
    for (i <- 1 to populationSize) {
      initialPopulationBuffer += initialPopulationIterator.next()
    }
    initialPopulationBuffer.toList
  }

  def addErrors(error1: List[Double], error2: List[Double]): List[Double] = {
    error1.zip(error2).map(errors => errors._1 + errors._2)
  }

  def stopOnIterationCount(maxIterationCount: Int, currentIteration: Int, scores: List[Double]) = {
     currentIteration >= maxIterationCount
  }

}
