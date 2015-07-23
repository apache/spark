package org.apache.spark.mllib.ann

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite


class ANNSuite extends FunSuite with MLlibTestSparkContext {

  // TODO: add test for uneven batching
  // TODO: add test for gradient

  test("ANN with Sigmoid learns XOR function with LBFGS optimizer") {
    val inputs = Array[Array[Double]](
      Array[Double](0, 0),
      Array[Double](0, 1),
      Array[Double](1, 0),
      Array[Double](1, 1)
    )
    val outputs = Array[Double](0, 1, 1, 0)
    val data = inputs.zip(outputs).map { case (features, label) =>
      (Vectors.dense(features), Vectors.dense(Array(label)))
    }
    val rddData = sc.parallelize(data, 1)
    val hiddenLayersTopology = Array[Int](5)
    val dataSample = rddData.first()
    val layerSizes = dataSample._1.size +: hiddenLayersTopology :+ dataSample._2.size
    val topology = FeedForwardTopology.multiLayerPerceptron(layerSizes, false)
    val initialWeights = FeedForwardModel(topology, 23124).weights()
    val trainer = new FeedForwardTrainer(topology, 2, 1)
    trainer.setWeights(initialWeights)
    trainer.LBFGSOptimizer.setNumIterations(20)
    val model = trainer.train(rddData)
    //val model = FeedForwardTrainer.train(rddData, 1, 20, topology, initialWeights)
    val predictionAndLabels = rddData.map { case (input, label) =>
      (model.predict(input)(0), label(0))
    }.collect()
    assert(predictionAndLabels.forall { case (p, l) => (math.round(p) - l) == 0})
  }

  test("ANN with SoftMax learns XOR function with 2-bit output and batch GD optimizer") {
    val inputs = Array[Array[Double]](
      Array[Double](0, 0),
      Array[Double](0, 1),
      Array[Double](1, 0),
      Array[Double](1, 1)
    )
    val outputs = Array[Array[Double]](
      Array[Double](1, 0),
      Array[Double](0, 1),
      Array[Double](0, 1),
      Array[Double](1, 0)
    )
    val data = inputs.zip(outputs).map { case (features, label) =>
      (Vectors.dense(features), Vectors.dense(label))
    }
    val rddData = sc.parallelize(data, 1)
    val hiddenLayersTopology = Array[Int](5)
    val dataSample = rddData.first()
    val layerSizes = dataSample._1.size +: hiddenLayersTopology :+ dataSample._2.size
    val topology = FeedForwardTopology.multiLayerPerceptron(layerSizes, false)
    val initialWeights = FeedForwardModel(topology, 23124).weights()
    val trainer = new FeedForwardTrainer(topology, 2, 2)
    trainer.SGDOptimizer.setNumIterations(2000)
    //trainer.LBFGSOptimizer.setNumIterations(100)
    trainer.setWeights(initialWeights)
    val model = trainer.train(rddData)
    val predictionAndLabels = rddData.map { case (input, label) =>
      (model.predict(input).toArray.map(math.round(_)), label.toArray)
    }.collect()
    assert(predictionAndLabels.forall { case (p, l) => p.deep == l.deep})
  }

}
