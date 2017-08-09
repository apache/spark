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

package org.apache.spark.ml.stat

import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD

/**
 * Summarizers provides helper functions to get feature and label summarizers from provided data.
 */
private[ml] object Summarizers extends Serializable {

  /** Get regression feature and label summarizers for provided data. */
  def getRegressionSummarizers(
      instances: RDD[Instance],
      aggregationDepth: Int = 2): (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer) = {
    val seqOp = (c: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
                 instance: Instance) =>
      (c._1.add(instance.features, instance.weight),
        c._2.add(Vectors.dense(instance.label), instance.weight))

    val combOp = (c1: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
                  c2: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer)) =>
      (c1._1.merge(c2._1), c1._2.merge(c2._2))

    instances.treeAggregate(
      new MultivariateOnlineSummarizer, new MultivariateOnlineSummarizer
    )(seqOp, combOp, aggregationDepth)
  }

  /** Get classification feature and label summarizers for provided data. */
  def getClassificationSummarizers(
      instances: RDD[Instance],
      aggregationDepth: Int = 2): (MultivariateOnlineSummarizer, MultiClassSummarizer) = {
    val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
                 instance: Instance) =>
      (c._1.add(instance.features, instance.weight),
        c._2.add(instance.label, instance.weight))

    val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
                  c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
      (c1._1.merge(c2._1), c1._2.merge(c2._2))

    instances.treeAggregate(
      new MultivariateOnlineSummarizer, new MultiClassSummarizer
    )(seqOp, combOp, aggregationDepth)
  }
}
