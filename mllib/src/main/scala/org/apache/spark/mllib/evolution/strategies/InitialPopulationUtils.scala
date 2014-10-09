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
 * Util class for individual selection and crossover applying implementations
 */
object InitialPopulationUtils {

  /**
   * Simple random initializator. Allow you to generate population
   * just with method of generating single individual. It's important because seed
   * of experiment should be brought everywhere to be able to reproduce(not supported yet)
   * @param generateSingle method for generating single individual
   * @return population of random generated individuals
   */
  def randomInitialization[I](generateSingle: Long => I): Long => Iterator[I] = {

    def result(seed: Long): Iterator[I] = {
      val random = new Random(seed)
      new Iterator[I] {
        var changableSeed = random.nextLong()

        def hasNext() = true

        def next(): I = {
          changableSeed = random.nextLong()
          generateSingle(changableSeed)
        }
      }
    }
    result
  }

}
