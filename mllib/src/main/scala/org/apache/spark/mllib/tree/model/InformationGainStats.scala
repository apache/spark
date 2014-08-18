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
 * @param predict predicted value
 * @param prob probability of the label (classification only)
 */
@DeveloperApi
class InformationGainStats(
    val gain: Double,
    val impurity: Double,
    val leftImpurity: Double,
    val rightImpurity: Double,
    val predict: Double,
    val prob: Double = 0.0) extends Serializable {

  override def toString = {
    "gain = %f, impurity = %f, left impurity = %f, right impurity = %f, predict = %f, prob = %f"
      .format(gain, impurity, leftImpurity, rightImpurity, predict, prob)
  }
}
