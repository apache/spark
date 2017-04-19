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

import org.apache.spark.mllib.evolution.strategies.{CrossoverUtils, InitialPopulationUtils}
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite

import scala.util.Random

class FindCorrectStringSuite extends FunSuite with LocalSparkContext {

  /**
   * Form string of "0" from random strings with "1" and "0"
   */
  test("Max ones test") {

    val individType = new ListOperations[Char, String](
      100,
      initialPopulationGeneratorOperation = {
        length =>
          InitialPopulationUtils.randomInitialization({
            seed =>
              val r = new Random(seed)
              (0 to length).map(_ => r.nextInt(2).toString()(0)).toList
          })
      },
      fitnessFunctionOperation = {
        (individ, train) => {
          individ.map(_.toString.toDouble).sum
        }
      }
    )

    val result = EvolutionRunner.run(
      sc.parallelize(List("")),
      100,
      100,
      individualType = individType
    )

    assert(result._2 < 15.0)
  }

  /**
   * Form string of "3" from random strings with "1" and "0", and mutation with alphabet of "3" and "1"
   */
  test("Mutation test") {

    val individType = new ListOperations[Char, String](
      100,
      initialPopulationGeneratorOperation = {
        length =>
          InitialPopulationUtils.randomInitialization({
            seed =>
              val r = new Random(seed)
              (0 to length).map(_ => r.nextInt(2).toString()(0)).toList
          })
      },
      fitnessFunctionOperation = {
        (individ, train) => {
          individ.map(i => if (i.toString.toInt < 3) 1 else 0).sum
        }
      },
      crossoverOperation = CrossoverUtils.crossoverWithMutation(List('1' ,'3'))(0.005)
    )

    val result = EvolutionRunner.run(
      sc.parallelize(List("")),
      100,
      100,
      individualType = individType
    )

    assert(result._2 < 15.0)
  }


}
