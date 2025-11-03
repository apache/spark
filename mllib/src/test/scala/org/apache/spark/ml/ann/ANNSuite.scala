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

package org.apache.spark.ml.ann

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.util.ArrayImplicits._

class ANNSuite extends SparkFunSuite with MLlibTestSparkContext {

  // TODO: test for weights comparison with Weka MLP
  test("ANN with Sigmoid learns XOR function with LBFGS optimizer") {
    val inputs = Array(
      Array(0.0, 0.0),
      Array(0.0, 1.0),
      Array(1.0, 0.0),
      Array(1.0, 1.0)
    )
    val outputs = Array(0.0, 1.0, 1.0, 0.0)
    val data = inputs.zip(outputs).map { case (features, label) =>
      (Vectors.dense(features), Vectors.dense(label))
    }
    val rddData = sc.parallelize(data.toImmutableArraySeq, 1)
    val hiddenLayersTopology = Array(5)
    val dataSample = rddData.first()
    val layerSizes = dataSample._1.size +: hiddenLayersTopology :+ dataSample._2.size
    val topology = FeedForwardTopology.multiLayerPerceptron(layerSizes, false)
    val initialWeights = FeedForwardModel(topology, 23124).weights
    val trainer = new FeedForwardTrainer(topology, 2, 1)
    trainer.setWeights(initialWeights)
    trainer.LBFGSOptimizer.setNumIterations(20)
    val (model, _) = trainer.train(rddData)
    val predictionAndLabels = rddData.map { case (input, label) =>
      (model.predict(input)(0), label(0))
    }.collect()
    predictionAndLabels.foreach { case (p, l) =>
      assert(math.round(p) === l)
    }
  }

  test("ANN with SoftMax learns XOR function with 2-bit output and batch GD optimizer") {
    val inputs = Array(
      Array(0.0, 0.0),
      Array(0.0, 1.0),
      Array(1.0, 0.0),
      Array(1.0, 1.0)
    )
    val outputs = Array(
      Array(1.0, 0.0),
      Array(0.0, 1.0),
      Array(0.0, 1.0),
      Array(1.0, 0.0)
    )
    val data = inputs.zip(outputs).map { case (features, label) =>
      (Vectors.dense(features), Vectors.dense(label))
    }
    val rddData = sc.parallelize(data.toImmutableArraySeq, 1)
    val hiddenLayersTopology = Array(5)
    val dataSample = rddData.first()
    val layerSizes = dataSample._1.size +: hiddenLayersTopology :+ dataSample._2.size
    val topology = FeedForwardTopology.multiLayerPerceptron(layerSizes, false)
    val initialWeights = FeedForwardModel(topology, 23124).weights
    val trainer = new FeedForwardTrainer(topology, 2, 2)
    // TODO: add a test for SGD
    trainer.LBFGSOptimizer.setConvergenceTol(1e-4).setNumIterations(20)
    trainer.setWeights(initialWeights).setStackSize(1)
    val (model, _) = trainer.train(rddData)
    val predictionAndLabels = rddData.map { case (input, label) =>
      (model.predict(input), label)
    }.collect()
    predictionAndLabels.foreach { case (p, l) =>
      assert(p ~== l absTol 0.5)
    }
  }
}
