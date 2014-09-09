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


package org.apache.spark.mllib.ann

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.mllib.linalg._

import scala.util.Random

object TestParallelANN {

  var rand = new Random(0)

  def generateInput2D(f: Double => Double, xmin: Double, xmax: Double, noPoints: Int):
  Array[(Vector, Vector)] = {

    var out = new Array[(Vector, Vector)](noPoints)

    for (i <- 0 to noPoints - 1) {
      val x = xmin + rand.nextDouble() * (xmax - xmin)
      val y = f(x)
      out(i) = (Vectors.dense(x), Vectors.dense(y))
    }

    return out

  }


  def generateInput3D(f: (Double, Double) => Double, xmin: Double, xmax: Double, ymin: Double, ymax: Double, noPoints: Int): Array[(Vector, Vector)] = {

    var out = new Array[(Vector, Vector)](noPoints)

    for (i <- 0 to noPoints - 1) {

      val x = xmin + rand.nextDouble() * (xmax - xmin)
      val y = ymin + rand.nextDouble() * (ymax - ymin)
      val z = f(x, y)

      var arr = new Array[Double](2)

      arr(0) = x
      arr(1) = y
      out(i) = (Vectors.dense(arr), Vectors.dense(z))

    }

    out

  }

  def generateInput4D(f: Double => (Double, Double, Double), tmin: Double, tmax: Double, noPoints: Int): Array[(Vector, Vector)] = {

    var out = new Array[(Vector, Vector)](noPoints)

    for (i <- 0 to noPoints - 1) {

      val t: Double = tmin + rand.nextDouble() * (tmax - tmin)
      var arr = new Array[Double](3)
      var F = f(t)

      arr(0) = F._1
      arr(1) = F._2
      arr(2) = F._3

      out(i) = (Vectors.dense(t), Vectors.dense(arr))
    }

    out

  }

  def f(T: Double): Double = {
    val y = 0.5 + Math.abs(T / 5).toInt.toDouble * .15 + math.sin(T * math.Pi / 10) * .1
    assert(y <= 1)
    y
  }

  def f3D(x: Double, y: Double): Double = {
    .5 +.24 * Math.sin(x * 2 * math.Pi / 10) +.24 * Math.cos(y * 2 * math.Pi / 10)
  }

  def f4D(t: Double): (Double, Double, Double) = {
    val x = Math.abs(.8 * Math.cos(t * 2 * math.Pi / 20)) + .1
    val y = (11 + t) / 22
    val z =.5 +.35 * Math.sin(t * 2 * math.Pi / 5) * Math.cos(t * 2 * math.Pi / 10) +.15 * t / 11
    (x, y, z)
  }

  def concat(v1: Vector, v2: Vector): Vector = {

    var a1 = v1.toArray
    var a2 = v2.toArray
    var a3 = new Array[Double](a1.size + a2.size)

    for (i <- 0 to a1.size - 1) {
      a3(i) = a1(i)
    }

    for (i <- 0 to a2.size - 1) {
      a3(i + a1.size) = a2(i)
    }

    Vectors.dense(a3)

  }

  def main(arg: Array[String]) {

    println("Parallel ANN tester")
    println

    val formatter = new SimpleDateFormat("hh:mm:ss")

    var curAngle: Double = 0.0
    var graphic: Boolean = false

    if ((arg.length > 0) && (arg(0) == "graph")) {
      graphic = true
    }

    var outputFrame2D: OutputFrame2D = null
    var outputFrame3D: OutputFrame3D = null
    var outputFrame4D: OutputFrame3D = null

    if (graphic) {

      outputFrame2D = new OutputFrame2D("x -> y")
      outputFrame2D.apply

      outputFrame3D = new OutputFrame3D("(x,y) -> z", 1)
      outputFrame3D.apply

      outputFrame4D = new OutputFrame3D("t -> (x,y,z)")
      outputFrame4D.apply

    }

    var A = 20.0
    var B = 50.0

    var conf = new SparkConf().setAppName("Parallel ANN").setMaster("local[1]")
    var sc = new SparkContext(conf)

    val testRDD2D = sc.parallelize(generateInput2D(T => f(T), -10, 10, 100), 2).cache
    val testRDD3D = sc.parallelize(generateInput3D((x, y) => f3D(x, y), -10, 10, -10, 10, 200), 2).cache
    val testRDD4D = sc.parallelize(generateInput4D(t => f4D(t), -10, 10, 100), 2).cache

    val validationRDD2D = sc.parallelize(generateInput2D(T => f(T), -10, 10, 100), 2).cache
    val validationRDD3D = sc.parallelize(generateInput3D((x, y) => f3D(x, y), -10, 10, -10, 10, 100), 2).cache
    val validationRDD4D = sc.parallelize(generateInput4D(t => f4D(t), -10, 10, 100), 2).cache

    if (graphic) {

      outputFrame2D.setData(testRDD2D.map(T => concat(T._1, T._2)))
      outputFrame3D.setData(testRDD3D.map(T => concat(T._1, T._2)))
      outputFrame4D.setData(testRDD4D.map(T => T._2))

    }

    val starttime = Calendar.getInstance().getTime()
    println("Start training " + starttime)

    val numIterations = 1000
    val stepSize = 1.0
    var model2D = ArtificialNeuralNetwork.train(testRDD2D, Array[Int](1, 3, 3, 1), numIterations, stepSize)
    var model3D = ArtificialNeuralNetwork.train(testRDD3D, Array[Int](2, 20, 1), numIterations, stepSize)
    var model4D = ArtificialNeuralNetwork.train(testRDD4D, Array[Int](1, 20, 3), numIterations, stepSize)

    val noIt = 1500
    var errHist = new Array[(Int, Double, Double, Double)](noIt)

    for (i <- 0 to noIt - 1) {

      val predictedAndTarget2D = validationRDD2D.map(T => (T._1, T._2, model2D.predictV(T._1)))
      val predictedAndTarget3D = validationRDD3D.map(T => (T._1, T._2, model3D.predictV(T._1)))
      val predictedAndTarget4D = validationRDD4D.map(T => (T._1, T._2, model4D.predictV(T._1)))

      var err2D = predictedAndTarget2D.map(T =>
        (T._3.toArray(0) - T._2.toArray(0)) * (T._3.toArray(0) - T._2.toArray(0))
      ).reduce((u, v) => u + v)

      var err3D = predictedAndTarget3D.map(T =>
        (T._3.toArray(0) - T._2.toArray(0)) * (T._3.toArray(0) - T._2.toArray(0))
      ).reduce((u, v) => u + v)

      var err4D = predictedAndTarget4D.map(T => {

        val v1 = T._2.toArray
        val v2 = T._3.toArray

        (v1(0) - v2(0)) * (v1(0) - v2(0)) +
          (v1(1) - v2(1)) * (v1(1) - v2(1)) +
          (v1(2) - v2(2)) * (v1(2) - v2(2))

      }).reduce((u, v) => u + v)


      if (graphic) {

        val predicted2D = predictedAndTarget2D.map(
          T => concat(T._1, T._3)
        )

        val predicted3D = predictedAndTarget3D.map(
          T => concat(T._1, T._3)
        )

        val predicted4D = predictedAndTarget4D.map(
          T => T._3
        )

        curAngle = curAngle + math.Pi / 4
        if (curAngle >= 2 * math.Pi) {
          curAngle = curAngle - 2 * math.Pi
        }

        outputFrame3D.setAngle(curAngle)
        outputFrame4D.setAngle(curAngle)

        outputFrame2D.setApproxPoints(predicted2D)
        outputFrame3D.setApproxPoints(predicted3D)
        outputFrame4D.setApproxPoints(predicted4D)

      }

      println("It. " + i + " (" + Calendar.getInstance().getTime() + "), Error 2D/3D/4D: " +(err2D, err3D, err4D))
      errHist(i) = (i, err2D, err3D, err4D)

      if (i < noIt - 1) {
        model2D = ArtificialNeuralNetwork.train(testRDD2D, model2D.topology, model2D.weights, numIterations, stepSize)
        model3D = ArtificialNeuralNetwork.train(testRDD3D, model3D.topology, model3D.weights, numIterations, stepSize)
        model4D = ArtificialNeuralNetwork.train(testRDD4D, model4D.topology, model4D.weights, numIterations, stepSize)
      }

    }

    sc.stop

    val stoptime = Calendar.getInstance().getTime()

    for (i <- 0 to noIt - 1) {
      println(errHist(i))
    }

    println(formatter.format(starttime) + "-" + formatter.format(stoptime) + " " + (stoptime.getTime - starttime.getTime + 500) / 1000 + " seconds")

  }

}
