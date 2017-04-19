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

package org.apache.spark.mllib.evolution.strategies

import scala.util.Random

/**
 * Util class for crossover implementations
 */
object CrossoverUtils {

  /**
   * Simple crossover for ListIndividual. Just randomly select part
   * from one list or another. Default for ListIndividual.
   * @param list1 first individual
   * @param list2 first individual
   * @return new individual - result from random merge of input individuals
   */
  def simpleCrossover[E](list1: List[E], list2: List[E]): List[List[E]] = {
    List(list1.zip(list2).map(e => if (Random.nextBoolean) e._1 else e._2))
  }

  /**
   * Simple crossover with mutation. Makes mutations accordingly to mutation factor.
   * When we mutate some element in list, we just randomly choose element from alphabet.
   * If do not mutate now we select part from one list or another.
   * @param alphabet alphabet of elements we can select from to choose a mutation
   * @param mutationFactor percent of mutated elements
   * @param list1 first individual
   * @param list2 second individual
   * @return  new individual - result from random merge of input individuals with mutation
   */
  def crossoverWithMutation[E]
            (alphabet: List[E])
            (mutationFactor: Double)
            (list1: List[E], list2: List[E]): List[List[E]] = {
    List(list1.zip(list2).map(e => if (Random.nextDouble() < mutationFactor) {
      alphabet(Random.nextInt(alphabet.size))
    } else {
      if (Random.nextBoolean) e._1 else e._2
    }))
  }

}
