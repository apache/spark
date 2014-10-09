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

import org.apache.spark.mllib.evolution.strategies.{SelectorType, CrossoverUtils}

/**
 * Special case of evolutionary computation is genetic algorithms, where individual
 * is array ot string.[[http://en.wikipedia.org/wiki/Selection_(genetic_algorithm)]]
 * This class defines this case
 * @param length length of individual list
 */
class ListOperations[E, T](
                    length: Int,
                    initialPopulationGeneratorOperation:
                        Int => ((Long) => Iterator[List[E]]),
                    fitnessFunctionOperation:
                        (List[E], T) => Double,
                    crossoverOperation:
                        (List[E], List[E]) => List[List[E]]
                        = CrossoverUtils.simpleCrossover[E] _,
                    selectionOperation:
                        (List[(List[E], Double)],
                          (List[E], List[E]) => List[List[E]]) => List[List[E]]
                        = SelectorType.simpleSelection[List[E], T] _
                         ) extends IndividualOperations[List[E], T] {

  override def crossover = crossoverOperation

  override def initialPopulationGenerator = initialPopulationGeneratorOperation(length)

  override def selection = selectionOperation

  override def fitnessFunction = fitnessFunctionOperation
}
