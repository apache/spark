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

package org.apache.spark.mllib.tree.model

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Information gain statistics for each split
 * @param gain information gain value
 * @param impurity current node impurity
 * @param leftImpurity left node impurity
 * @param rightImpurity right node impurity
 * @param leftPredict left node predict
 * @param rightPredict right node predict
 */
@DeveloperApi
class InformationGainStats(
    val gain: Double,
    val impurity: Double,
    val leftImpurity: Double,
    val rightImpurity: Double,
    val leftPredict: Predict,
    val rightPredict: Predict) extends Serializable {

  override def toString: String = {
    s"gain = $gain, impurity = $impurity, left impurity = $leftImpurity, " +
      s"right impurity = $rightImpurity"
  }

  override def equals(o: Any): Boolean = o match {
    case other: InformationGainStats =>
      gain == other.gain &&
      impurity == other.impurity &&
      leftImpurity == other.leftImpurity &&
      rightImpurity == other.rightImpurity &&
      leftPredict == other.leftPredict &&
      rightPredict == other.rightPredict

    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(
      gain: java.lang.Double,
      impurity: java.lang.Double,
      leftImpurity: java.lang.Double,
      rightImpurity: java.lang.Double,
      leftPredict,
      rightPredict)
  }
}


private[tree] object InformationGainStats {
  /**
   * An [[org.apache.spark.mllib.tree.model.InformationGainStats]] object to
   * denote that current split doesn't satisfies minimum info gain or
   * minimum number of instances per node.
   */
  val invalidInformationGainStats = new InformationGainStats(Double.MinValue, -1.0, -1.0, -1.0,
    new Predict(0.0, 0.0), new Predict(0.0, 0.0))
}
