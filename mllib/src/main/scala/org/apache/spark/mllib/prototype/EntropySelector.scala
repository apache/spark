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

package org.apache.spark.mllib.prototype

import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

/**
 * Basic skeleton of an entropy based
 * subset selector
 */
abstract class EntropySelector
  extends SubsetSelector[(Long, Vector)] with Serializable
  with Logging {
  protected val measure: EntropyMeasure
  protected val delta: Double
  protected val MAX_ITERATIONS: Int
}

class GreedyEntropySelector(m: EntropyMeasure,
                            del: Double = 0.0001,
                            max: Int = 5000)
  extends EntropySelector with Serializable
  with Logging {

  override protected val measure: EntropyMeasure = m
  override protected val delta: Double = del
  override protected val MAX_ITERATIONS: Int =  max

  override def selectPrototypes(data: RDD[(Long, Vector)],
                                M: Int): RDD[(Long, Vector)] = {

    val context = data.context

    /*
    * Draw an initial sample of M points
    * from data without replacement.
    *
    * Define a working set which we
    * will use as a prototype set to
    * to each iteration
    * */

    val workingset = data.keys.takeSample(false, M)

    val r = scala.util.Random
    var it: Int = 0

    //All the elements not in the working set
    var newDataset: RDD[Long] = data.keys.filter((p) => !workingset.contains(p))
    //Existing best value of the entropy
    var oldEntropy: Double = this.measure.evaluate(data.filter((point) =>
      workingset.contains(point._1)))
    //Store the value of entropy after an element swap
    var newEntropy: Double = 0.0
    var d: Double = Double.NegativeInfinity
    var rand: Int = 0
    do {
      /*
       * Randomly select a point from
       * the working set as well as data
       * and then swap them.
       * */
      rand = r.nextInt(workingset.length - 1)
      val point1 = workingset.apply(rand)

      val point2 = newDataset.takeSample(false, 1).apply(0)

      //Update the working set
      workingset(rand) = point2
      //Calculate the new entropy
      newEntropy = this.measure.evaluate(data.filter((p) =>
        workingset.contains(p._1)))

      /*
      * Calculate the change in entropy,
      * if it has improved then keep the
      * swap, otherwise revert to existing
      * working set.
      * */
      d = newEntropy - oldEntropy

      if(d < 0) {
        /*
        * Improvement in entropy so
        * keep the updated working set
        * as it is and update the
        * variable 'newDataset'
        * */
        oldEntropy = newEntropy
        newDataset = data.keys.filter((p) => !workingset.contains(p))
      } else {
        /*
        * No improvement in entropy
        * so revert the working set
        * to its initial state. Leave
        * the variable newDataset as
        * it is.
        * */
        workingset(rand) = point1
      }

      it += 1
    } while(math.abs(d) >= this.delta &&
      it <= this.MAX_ITERATIONS)

    //Time to return the final working set
    data.filter((p) => workingset.contains(p._1))
  }

}
