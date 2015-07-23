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
package org.apache.spark.mllib.neuralNetwork

import java.util.{ArrayList, List}

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, _}
import breeze.numerics.sigmoid
import org.apache.spark.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Builder class to define CNN layers structure
 * The second layer from the last can not be a conv layer,
 * Typical layers: input, conv, samp, conv, samp, output
 */
class CNNTopology {
  var mLayers: List[CNNLayer] = new ArrayList[CNNLayer]

  def this(layer: CNNLayer) {
    this()
    mLayers.add(layer)
  }

  def addLayer(layer: CNNLayer): CNNTopology = {
    mLayers.add(layer)
    this
  }
}

/**
 * Convolution neural network
 */
class CNN private extends Serializable with Logging{
  private var ALPHA: Double = 0.85
  private var layers: List[CNNLayer] = null
  private var layerNum: Int = 0
  private var maxIterations = 10
  private var batchSize = 100

  def this(layerBuilder: CNNTopology) {
    this()
    layers = layerBuilder.mLayers
    layerNum = layers.size
    setup
  }

  def setup {
    var i: Int = 1
    while (i < layers.size) {
      val layer: CNNLayer = layers.get(i)
      val frontLayer: CNNLayer = layers.get(i - 1)
      val frontMapNum: Int = frontLayer.getOutMapNum
      layer.getType match {
        case "input" =>
        case "conv" =>
          val convLayer = layer.asInstanceOf[ConvCNNLayer]
          convLayer.setMapSize(frontLayer.getMapSize.subtract(convLayer.getKernelSize, 1))
          convLayer.initKernel(frontMapNum)
          convLayer.initBias(frontMapNum)
        case "samp" =>
          val sampLayer = layer.asInstanceOf[SampCNNLayer]
          sampLayer.setOutMapNum(frontMapNum)
          sampLayer.setMapSize(frontLayer.getMapSize.divide(sampLayer.getScaleSize))
        case "output" =>
          val outputLayer = layer.asInstanceOf[OutputCNNLayer]
          outputLayer.initOutputKernels(frontMapNum, frontLayer.getMapSize)
          outputLayer.asInstanceOf[OutputCNNLayer].initBias(frontMapNum)
      }
      i += 1
    }
  }

  def setMiniBatchSize(batchSize: Int): this.type = {
    this.batchSize = batchSize
    this
  }

  /**
   * Maximum number of iterations for learning.
   */
  def getMaxIterations: Int = maxIterations

  /**
   * Maximum number of iterations for learning.
   * (default = 20)
   */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  def trainOneByOne(trainSet: RDD[LabeledPoint]) {
    var t = 0
    val trainSize = trainSet.count().toInt
    val dataArr = trainSet.collect()
    while (t < maxIterations) {
      val epochsNum = trainSize
      var right = 0
      var count = 0
      var i = 0
      while (i < epochsNum) {
        val record = dataArr(i)
        val result = train(record)
        if (result._1) right += 1
        count += 1
        val gradient: Array[(Array[Array[BDM[Double]]], Array[Double])] = result._2
        updateParams(gradient, 1)
        i += 1
      }
      val p = 1.0 * right / count
      if (t % 10 == 1 && p > 0.96) {
        ALPHA = 0.001 + ALPHA * 0.9
      }
      t += 1
      logInfo(s"precision $right/$count = $p")
    }
  }

  def train(trainSet: RDD[LabeledPoint]) {
    var t = 0
    val trainSize = trainSet.count().toInt
    val gZero = train(trainSet.first)._2
    gZero.foreach(tu => if (tu != null){
        tu._1.foreach(m => m.foreach(x => x -= x))
        (0 until tu._2.length).foreach(i => tu._2(i) = 0)
      }
    )
    var totalCount = 0
    var totalRight = 0
    while (t < maxIterations) {
      val (gradientSum, right, count) = trainSet
        .sample(false, batchSize.toDouble/trainSize, 42 + t)
        .treeAggregate((gZero, 0, 0))(
          seqOp = (c, v) => {
            val result = train(v)
            val gradient = result._2
            val right = if (result._1) 1 else 0
            (CNN.combineGradient(c._1, gradient), c._2 + right, c._3 + 1)
          },
          combOp = (c1, c2) => {
            (CNN.combineGradient(c1._1, c2._1), c1._2 + c2._2, c1._3 + c2._3)
          })

      t += 1
      if (count > 0){
        updateParams(gradientSum, count)
        val p = 1.0 * totalRight / totalCount
        if (t % 10 == 1 && p > 0.96) {
          ALPHA = 0.001 + ALPHA * 0.9
        }
        totalCount += count
        totalRight += right
        if (totalCount > 10000){
          logInfo(s"precision $totalRight/$totalCount = $p")
          totalCount = 0
          totalRight = 0
        }
      }
    }
  }

  def predict(testSet: RDD[Vector]): RDD[Int] = {
    testSet.map(record => {
      val outputs: Array[Array[BDM[Double]]] = forward(record)
      val outputLayer = layers.get(layerNum - 1)
      val mapNum = outputLayer.getOutMapNum
      val out = new Array[Double](mapNum)
      for (m <- 0 until mapNum) {
        val outMap = outputs(layerNum - 1)(m)
        out(m) = outMap(0, 0)
      }
      CNN.getMaxIndex(out)
    })
  }

  /**
   * train one record
   *
   * @return (isRight, gradient)
   */
  private def train(
      record: LabeledPoint): (Boolean, Array[(Array[Array[BDM[Double]]], Array[Double])]) = {
    val outputs = forward(record.features)
    val (right, errors) = backPropagation(record, outputs)
    val gradient = getGradient(outputs, errors)
    (right, gradient)
  }

  /**
   * forward for one record
   *
   * @param record
   */
  private def forward(record: Vector): Array[Array[BDM[Double]]] = {
    val outputs = new Array[Array[BDM[Double]]](layers.size)
    outputs(0) = setInLayerOutput(record)
    var l: Int = 1
    while (l < layers.size) {
      val layer: CNNLayer = layers.get(l)
      outputs(l) =
        layer.getType match {
          case "conv" =>
            setConvOutput(layer.asInstanceOf[ConvCNNLayer], outputs(l - 1))
          case "samp" =>
            setSampOutput(layer.asInstanceOf[SampCNNLayer], outputs(l - 1))
          case "output" =>
            setConvOutput(layer.asInstanceOf[ConvCNNLayer], outputs(l - 1))
          case _ => null
        }
      l += 1
    }
    outputs
  }

  /**
   * run BP and get errors for all layers
   *
   * @return (right, errors for all layers)
   */
  private def backPropagation(
      record: LabeledPoint,
      outputs: Array[Array[BDM[Double]]]): (Boolean, Array[Array[BDM[Double]]]) = {
    val errors = new Array[Array[BDM[Double]]](layers.size)
    val result = setOutLayerErrors(record, outputs(layerNum - 1))
    errors(layerNum - 1) = result._2
    var l: Int = layerNum - 2
    while (l > 0) {
      val layer: CNNLayer = layers.get(l)
      val nextLayer: CNNLayer = layers.get(l + 1)
      errors(l) = layer.getType match {
        case "samp" =>
          setSampErrors(layer, nextLayer.asInstanceOf[ConvCNNLayer], errors(l + 1))
        case "conv" =>
          setConvErrors(layer, nextLayer.asInstanceOf[SampCNNLayer], errors(l + 1), outputs(l))
        case _ => null
      }
      l -= 1
    }
    (result._1, errors)
  }


  private def getGradient(
      outputs: Array[Array[BDM[Double]]],
      errors: Array[Array[BDM[Double]]]) : Array[(Array[Array[BDM[Double]]], Array[Double])] = {
    var l: Int = 1
    val gradient = new Array[(Array[Array[BDM[Double]]], Array[Double])](layerNum)
    while (l < layerNum) {
      val layer: CNNLayer = layers.get(l)
      val lastLayer: CNNLayer = layers.get(l - 1)
      gradient(l) = layer.getType match {
        case "conv" =>
          val kernelGradient = getKernelsGradient(layer, lastLayer, errors(l), outputs(l - 1))
          val biasGradient = getBiasGradient(layer, errors(l))
          (kernelGradient, biasGradient)
        case "output" =>
          val kernelGradient = getKernelsGradient(layer, lastLayer, errors(l), outputs(l - 1))
          val biasGradient = getBiasGradient(layer, errors(l))
          (kernelGradient, biasGradient)
        case _ => null
      }
      l += 1
    }
    gradient
  }

  private def updateParams(
      gradient: Array[(Array[Array[BDM[Double]]],
      Array[Double])],
      batchSize: Int): Unit = {
    var l: Int = 1
    while (l < layerNum) {
      val layer: CNNLayer = layers.get(l)
      layer.getType match {
        case "conv" =>
          updateKernels(layer.asInstanceOf[ConvCNNLayer], gradient(l)._1, batchSize)
          updateBias(layer.asInstanceOf[ConvCNNLayer], gradient(l)._2, batchSize)
        case "output" =>
          updateKernels(layer.asInstanceOf[ConvCNNLayer], gradient(l)._1, batchSize)
          updateBias(layer.asInstanceOf[ConvCNNLayer], gradient(l)._2, batchSize)
        case _ =>
      }
      l += 1
    }
  }

  private def updateKernels(
      layer: ConvCNNLayer,
      gradient: Array[Array[BDM[Double]]], batchSize: Int): Unit = {
    val len = gradient.length
    val width = gradient(0).length
    var j = 0
    while (j < width) {
      var i = 0
      while (i < len) {
        // update kernel
        val deltaKernel = gradient(i)(j) / batchSize.toDouble * ALPHA
        layer.getKernel(i, j) += deltaKernel
        i += 1
      }
      j += 1
    }
  }

  private def updateBias(layer: ConvCNNLayer, gradient: Array[Double], batchSize: Int): Unit = {
    val gv = new BDV[Double](gradient)
    layer.getBias += gv * ALPHA / batchSize.toDouble
  }

  /**
   * get bias gradient
   *
   * @param layer layer to be updated
   * @param errors errors of this layer
   */
  private def getBiasGradient(layer: CNNLayer, errors: Array[BDM[Double]]): Array[Double] = {
    val mapNum: Int = layer.getOutMapNum
    var j: Int = 0
    val gradient = new Array[Double](mapNum)
    while (j < mapNum) {
      val error: BDM[Double] = errors(j)
      val deltaBias: Double = sum(error)
      gradient(j) = deltaBias
      j += 1
    }
    gradient
  }

  /**
   * get kernels gradient
   *
   * @param layer
   * @param lastLayer
   */
  private def getKernelsGradient(
      layer: CNNLayer,
      lastLayer: CNNLayer,
      layerError: Array[BDM[Double]],
      lastOutput: Array[BDM[Double]]): Array[Array[BDM[Double]]] = {
    val mapNum: Int = layer.getOutMapNum
    val lastMapNum: Int = lastLayer.getOutMapNum
    val delta = Array.ofDim[BDM[Double]](lastMapNum, mapNum)
    var j = 0
    while (j < mapNum) {
      var i = 0
      while (i < lastMapNum) {
        val error = layerError(j)
        val deltaKernel = CNN.convnValid(lastOutput(i), error)
        delta(i)(j) = deltaKernel
        i += 1
      }
      j += 1
    }
    delta
  }

  /**
   * set errors for sampling layer
   *
   * @param layer
   * @param nextLayer
   */
  private def setSampErrors(
      layer: CNNLayer,
      nextLayer: ConvCNNLayer,
      nextLayerError: Array[BDM[Double]]): Array[BDM[Double]] = {
    val mapNum: Int = layer.getOutMapNum
    val nextMapNum: Int = nextLayer.getOutMapNum
    val errors = new Array[BDM[Double]](mapNum)
    var i = 0
    while (i < mapNum) {
      var sum: BDM[Double] = null // sum for every kernel
      var j = 0
      while (j < nextMapNum) {
        val nextError = nextLayerError(j)
        val kernel = nextLayer.getKernel(i, j)
        // rotate kernel by 180 degrees and get full convolution
        if (sum == null) {
          sum = CNN.convnFull(nextError, flipud(fliplr(kernel)))
        }
        else {
          sum += CNN.convnFull(nextError, flipud(fliplr(kernel)))
        }
        j += 1
      }
      errors(i) = sum
      i += 1
    }
    errors
  }

  /**
   * set errors for convolution layer
   *
   * @param layer
   * @param nextLayer
   */
  private def setConvErrors(
      layer: CNNLayer,
      nextLayer: SampCNNLayer,
      nextLayerError: Array[BDM[Double]],
      layerOutput: Array[BDM[Double]]): Array[BDM[Double]] = {
    val mapNum: Int = layer.getOutMapNum
    val errors = new Array[BDM[Double]](mapNum)
    var m: Int = 0
    while (m < mapNum) {
      val scale: Scale = nextLayer.getScaleSize
      val nextError: BDM[Double] = nextLayerError(m)
      val map: BDM[Double] = layerOutput(m)
      var outMatrix: BDM[Double] = (1.0 - map)
      outMatrix = map :* outMatrix
      outMatrix = outMatrix :* CNN.kronecker(nextError, scale)
      errors(m) = outMatrix
      m += 1
    }
    errors
  }

  /**
   * set errors for output layer
   *
   * @param record
   * @return
   */
  private def setOutLayerErrors(
      record: LabeledPoint,
      output: Array[BDM[Double]]): (Boolean, Array[BDM[Double]]) = {
    val outputLayer: CNNLayer = layers.get(layerNum - 1)
    val mapNum: Int = outputLayer.getOutMapNum
    val target: Array[Double] = new Array[Double](mapNum)
    val outValues: Array[Double] = output.map(m => m(0, 0))

    val label = record.label.toInt
    target(label) = 1
    val layerError: Array[BDM[Double]] = (0 until mapNum).map(i => {
      val errorMatrix = new BDM[Double](1, 1)
      errorMatrix(0, 0) = outValues(i) * (1 - outValues(i)) * (target(i) - outValues(i))
      errorMatrix
    }).toArray
    val outClass = CNN.getMaxIndex(outValues)
    (label == outClass, layerError)
  }

  /**
   * set inlayer output
   * @param record
   */
  private def setInLayerOutput(record: Vector): Array[BDM[Double]] = {
    val inputLayer: CNNLayer = layers.get(0)
    val mapSize = inputLayer.getMapSize
    if (record.size != mapSize.x * mapSize.y) {
      throw new RuntimeException("data size and map size mismatch!")
    }
    val m = new BDM[Double](mapSize.x, mapSize.y)
    var i: Int = 0
    while (i < mapSize.x) {
      var j: Int = 0
      while (j < mapSize.y) {
        m(i, j) = record(mapSize.x * i + j)
        j += 1
      }
      i += 1
    }
    Array(m)
  }

  private def setConvOutput(
      layer: ConvCNNLayer,
      outputs: Array[BDM[Double]]): Array[BDM[Double]] = {
    val mapNum: Int = layer.getOutMapNum
    val lastMapNum: Int = outputs.length
    val output = new Array[BDM[Double]](mapNum)
    var j = 0
    val oldBias = layer.getBias
    while (j < mapNum) {
      var sum: BDM[Double] = null
      var i = 0
      while (i < lastMapNum) {
        val lastMap = outputs(i)
        val kernel = layer.getKernel(i, j)
        if (sum == null) {
          sum = CNN.convnValid(lastMap, kernel)
        }
        else {
          sum += CNN.convnValid(lastMap, kernel)
        }
        i += 1
      }
      sum = sigmoid(sum + oldBias(j))
      output(j) = sum
      j += 1
    }
    output
  }

  /**
   * set output for sampling layer
   *
   * @param layer
   */
  private def setSampOutput(
      layer: SampCNNLayer,
      outputs: Array[BDM[Double]]): Array[BDM[Double]] = {
    val lastMapNum: Int = outputs.length
    val output = new Array[BDM[Double]](lastMapNum)
    var i: Int = 0
    while (i < lastMapNum) {
      val lastMap: BDM[Double] = outputs(i)
      val scaleSize: Scale = layer.getScaleSize
      output(i) = CNN.scaleMatrix(lastMap, scaleSize)
      i += 1
    }
    output
  }
}


object CNN {

  private[neuralNetwork] def kronecker(matrix: BDM[Double], scale: Scale): BDM[Double] = {
    val ones = BDM.ones[Double](scale.x, scale.y)
    kron(matrix, ones)
  }

  /**
   * return a new matrix that has been scaled down
   *
   * @param matrix
   */
  private[neuralNetwork] def scaleMatrix(matrix: BDM[Double], scale: Scale): BDM[Double] = {
    val m: Int = matrix.rows
    val n: Int = matrix.cols
    val sm: Int = m / scale.x
    val sn: Int = n / scale.y
    val outMatrix = new BDM[Double](sm, sn)
    val size = scale.x * scale.y
    var i = 0
    while (i < sm) {
      var j = 0
      while (j < sn) {
        var sum = 0.0
        var si = i * scale.x
        while (si < (i + 1) * scale.x) {
          var sj = j * scale.y
          while (sj < (j + 1) * scale.y) {
            sum += matrix(si, sj)
            sj += 1
          }
          si += 1
        }
        outMatrix(i, j) = sum / size
        j += 1
      }
      i += 1
    }
    outMatrix
  }

  /**
   * full conv
   *
   * @param matrix
   * @param kernel
   * @return
   */
  private[neuralNetwork] def convnFull(matrix: BDM[Double], kernel: BDM[Double]): BDM[Double] = {
    val m: Int = matrix.rows
    val n: Int = matrix.cols
    val km: Int = kernel.rows
    val kn: Int = kernel.cols
    val extendMatrix = new BDM[Double](m + 2 * (km - 1), n + 2 * (kn - 1))
    var i = 0
    var j = 0
    while (i < m) {
      while (j < n) {
        extendMatrix(i + km - 1, j + kn - 1) = matrix(i, j)
        j += 1
      }
      i += 1
    }
    convnValid(extendMatrix, kernel)
  }

  /**
   * valid conv
   *
   * @param matrix
   * @param kernel
   * @return
   */
  private[neuralNetwork] def convnValid(matrix: BDM[Double], kernel: BDM[Double]): BDM[Double] = {
    val m: Int = matrix.rows
    val n: Int = matrix.cols
    val km: Int = kernel.rows
    val kn: Int = kernel.cols
    val kns: Int = n - kn + 1
    val kms: Int = m - km + 1
    val outMatrix: BDM[Double] = new BDM[Double](kms, kns)
    var i = 0
    while (i < kms) {
      var j = 0
      while (j < kns) {
        var sum = 0.0
        for (ki <- 0 until km) {
          for (kj <- 0 until kn)
            sum += matrix(i + ki, j + kj) * kernel(ki, kj)
        }
        outMatrix(i, j) = sum
        j += 1
      }
      i += 1
    }
    outMatrix
  }

  private[neuralNetwork] def getMaxIndex(out: Array[Double]): Int = {
    var max: Double = out(0)
    var index: Int = 0
    var i: Int = 1
    while (i < out.length) {
      if (out(i) > max) {
        max = out(i)
        index = i
      }
      i += 1
    }
    index
  }

  private[neuralNetwork] def combineGradient(
      g1: Array[(Array[Array[BDM[Double]]], Array[Double])],
      g2: Array[(Array[Array[BDM[Double]]], Array[Double])]):
      Array[(Array[Array[BDM[Double]]], Array[Double])] = {

    val l = g1.length
    var li = 0
    while(li < l){
      if (g1(li) != null){
        // kernel
        val layer = g1(li)._1
        val x = layer.length
        var xi = 0
        while(xi < x){
          val line: Array[BDM[Double]] = layer(xi)
          val y = line.length
          var yi = 0
          while(yi < y){
            line(yi) += g2(li)._1(xi)(yi)
            yi += 1
          }
          xi += 1
        }

        // bias
        val b = g1(li)._2
        val len = b.length
        var bi = 0
        while(bi < len){
          b(bi) = b(bi) + g2(li)._2(bi)
          bi += 1
        }
      }
      li += 1
    }
    g1
  }
}
