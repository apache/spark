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

import java.io.Serializable

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

object CNNLayer {

  def buildInputLayer(mapSize: Scale): CNNLayer = {
    val layer: CNNLayer = new InputCNNLayer
    layer.outMapNum = 1
    layer.setMapSize(mapSize)
    layer
  }

  def buildConvLayer(outMapNum: Int, kernelSize: Scale): CNNLayer = {
    val layer = new ConvCNNLayer
    layer.outMapNum = outMapNum
    layer.setKernelSize(kernelSize)
    layer
  }

  def buildSampLayer(scaleSize: Scale): CNNLayer = {
    val layer = new SampCNNLayer
    layer.setScaleSize(scaleSize)
    layer
  }

  def buildOutputLayer(classNum: Int): CNNLayer = {
    val layer = new OutputCNNLayer
    layer.mapSize = new Scale(1, 1)
    layer.outMapNum = classNum
    layer
  }
}

/**
 * scale size for conv and sampling, can have different x and y
 */
class Scale(var x: Int, var y: Int) extends Serializable {

  /**
   * divide a scale with other scale
   *
   * @param scaleSize
   * @return
   */
  private[neuralNetwork] def divide(scaleSize: Scale): Scale = {
    val x: Int = this.x / scaleSize.x
    val y: Int = this.y / scaleSize.y
    if (x * scaleSize.x != this.x || y * scaleSize.y != this.y){
      throw new RuntimeException(this + "can not be divided" + scaleSize)
    }
    new Scale(x, y)
  }

  /**
   * subtract a scale and add append
   */
  private[neuralNetwork] def subtract(other: Scale, append: Int): Scale = {
    val x: Int = this.x - other.x + append
    val y: Int = this.y - other.y + append
    new Scale(x, y)
  }
}

abstract class CNNLayer private[neuralNetwork] extends Serializable {

  protected var layerType: String = null
  protected var outMapNum: Int = 0
  private var mapSize: Scale = null

  def getOutMapNum: Int = outMapNum
  def setOutMapNum(value: Int): this.type = {
    this.outMapNum = value
    this
  }

  def getMapSize: Scale = mapSize
  def setMapSize(mapSize: Scale): this.type = {
    this.mapSize = mapSize
    this
  }

  def getType: String = {
    layerType
  }
}

class InputCNNLayer extends CNNLayer{
  this.layerType = "input"
}

class ConvCNNLayer private[neuralNetwork] extends CNNLayer{
  private var bias: BDV[Double] = null
  private var kernel: Array[Array[BDM[Double]]] = null
  private var kernelSize: Scale = null

  this.layerType = "conv"
  private[neuralNetwork] def initBias(frontMapNum: Int) {
    this.bias = BDV.zeros[Double](outMapNum)
  }

  private[neuralNetwork] def initKernel(frontMapNum: Int) {
    this.kernel = Array.ofDim[BDM[Double]](frontMapNum, outMapNum)
    for (i <- 0 until frontMapNum)
      for (j <- 0 until outMapNum)
        kernel(i)(j) = (BDM.rand[Double](kernelSize.x, kernelSize.y) - 0.05) / 10.0
  }

  def getBias: BDV[Double] = bias
  def setBias(mapNo: Int, value: Double): this.type = {
    bias(mapNo) = value
    this
  }

  def getKernelSize: Scale = kernelSize
  def setKernelSize(value: Scale): this.type = {
    this.kernelSize = value
    this
  }

  def getKernel(i: Int, j: Int): BDM[Double] = kernel(i)(j)
}

class SampCNNLayer private[neuralNetwork] extends CNNLayer{
  private var scaleSize: Scale = null
  this.layerType = "samp"

  def getScaleSize: Scale = scaleSize
  def setScaleSize(value: Scale): this.type = {
    this.scaleSize = value
    this
  }
}

class OutputCNNLayer private[neuralNetwork] extends ConvCNNLayer{
  this.layerType = "output"
  /**
   * kernel size for output layer is equal to map size of last layer
   *
   * @param frontMapNum
   * @param size
   */
  private[neuralNetwork] def initOutputKernels(frontMapNum: Int, size: Scale) {
    this.setKernelSize(size)
    this.initKernel(frontMapNum)
  }
}
