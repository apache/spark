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

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
 * Util class for individual selection and crossover applying implementations
 */
object SelectorType {

  /**
   * Simple selection for ListIndividual. We select best 10 percent individuals - top10. 
   * We produce next population by taking top10 and combining these top10.
   * @param currentPopulation current population
   * @param crossover crossover operation which we should apply
   * @return new population
   */
  def simpleSelection[I, T](
                             currentPopulation: List[(I, Double)],
                             crossover: (I, I) => List[I]): List[I] = {

    val top10Percent = currentPopulation.sortBy(_._2).map(_._1).take(currentPopulation.size / 10)

    val result = new ListBuffer[I]
    result ++= top10Percent

    for (i <- 0 to 9) {
      result ++= top10Percent
        .zip(Random.shuffle(top10Percent))
        .map(i => crossover(i._1, i._2).head)
    }

    result.toList.take(currentPopulation.size)
  }

}
