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

package org.apache.spark.ml.tuning

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.{ParamMap, ParamPair, Params}

object ValidatorParamsSuiteHelpers extends SparkFunSuite {
  /**
   * Assert sequences of estimatorParamMaps are identical.
   * If the values for a parameter are not directly comparable with ===
   * and are instead Params types themselves then their corresponding paramMaps
   * are compared against each other.
   */
  def compareParamMaps(pMaps: Array[ParamMap], pMaps2: Array[ParamMap]): Unit = {
    assert(pMaps.length === pMaps2.length)
    pMaps.zip(pMaps2).foreach { case (pMap, pMap2) =>
      assert(pMap.size === pMap2.size)
      pMap.toSeq.foreach { case ParamPair(p, v) =>
        assert(pMap2.contains(p))
        val otherParam = pMap2(p)
        v match {
          case estimator: Params =>
            otherParam match {
              case estimator2: Params =>
                val estimatorParamMap = Array(estimator.extractParamMap())
                val estimatorParamMap2 = Array(estimator2.extractParamMap())
                compareParamMaps(estimatorParamMap, estimatorParamMap2)
              case other =>
                throw new AssertionError(s"Expected parameter of type Params but" +
                  s" found ${otherParam.getClass.getName}")
            }
          case _ =>
            assert(otherParam === v)
        }
      }
    }
  }
}
