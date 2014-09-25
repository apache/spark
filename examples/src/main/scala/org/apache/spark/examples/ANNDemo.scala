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

package org.apache.spark.examples.mllib

import java.awt._
import java.awt.event._
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.mllib.ann._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD

import scala.Array.canBuildFrom
import scala.util.Random

object windowAdapter extends WindowAdapter {

  override def windowClosing(e: WindowEvent) {
    System.exit(0)
  }

}

class OutputCanvas2D(wd: Int, ht: Int) extends Canvas {

  var points: Array[Vector] = null
  var approxPoints: Array[Vector] = null

  /* input: rdd of (x,y) vectors */
  def setData(rdd: RDD[Vector]) {
    points = rdd.collect
    repaint
  }

  def setApproxPoints(rdd: RDD[Vector]) {
    approxPoints = rdd.collect
    repaint
  }

  def plotDot(g: Graphics, x: Int, y: Int) {
    val r = 5
    val noSamp = 6*r
    var x1 = x
    var y1 = y + r
    for(j <- 1 to noSamp) {
      val x2 = (x.toDouble + math.sin(j.toDouble*2*math.Pi/noSamp)*r + .5).toInt
      val y2 = (y.toDouble + math.cos(j.toDouble*2*math.Pi/noSamp)*r + .5).toInt
      g.drawLine(x1, ht - y1, x2, ht - y2)
      x1 = x2
      y1 = y2
    }
  }

  override def paint(g: Graphics) = {

    var xmax: Double = 0.0
    var xmin: Double = 0.0
    var ymax: Double = 0.0
    var ymin: Double = 0.0

    if(points!=null) {

      g.setColor(Color.black)
      val x = points.map(T => (T.toArray)(0))
      val y = points.map(T => (T.toArray)(1))

      xmax = x.max
      xmin = x.min
      ymax = y.max
      ymin = y.min

      for(i <- 0 to x.size - 1) {

        val xr = (((x(i).toDouble - xmin)/(xmax - xmin))*wd + .5).toInt
        val yr = (((y(i).toDouble - ymin)/(ymax - ymin))*ht + .5).toInt
        plotDot(g, xr, yr)

      }

      if(approxPoints != null) {

        g.setColor(Color.red)
        val x = approxPoints.map(T => (T.toArray)(0))
        val y = approxPoints.map(T => (T.toArray)(1))

        for(i <- 0 to x.size-1) {
          val xr = (((x(i).toDouble - xmin)/(xmax - xmin))*wd + .5).toInt
          val yr = (((y(i).toDouble - ymin)/(ymax - ymin))*ht + .5).toInt
          plotDot(g, xr, yr)
        }

      }

    }

  }

}

class OutputFrame2D( title: String ) extends Frame( title ) {

  val wd = 800
  val ht = 600

  var outputCanvas = new OutputCanvas2D( wd, ht )

  def apply() {
    addWindowListener(windowAdapter)
    setSize(wd, ht)
    add("Center", outputCanvas)
    show()
  }

  def setData(rdd: RDD[Vector]) {
    outputCanvas.setData(rdd)
  }

  def setApproxPoints(rdd: RDD[Vector]) {
    outputCanvas.setApproxPoints(rdd)
  }


}

object windowAdapter3D extends WindowAdapter {

  override def windowClosing(e: WindowEvent) {
    System.exit(0)
  }

}

class OutputCanvas3D(wd: Int, ht: Int, shadowFrac: Double) extends Canvas {

  var points: Array[Vector] = null
  var approxPoints: Array[Vector] = null
  var angle: Double = 0.0

  /* 3 dimensional (x,y,z) vector */
  def setData(rdd: RDD[Vector]) {
    points = rdd.collect
    repaint
  }

  def setAngle(angle: Double) {
    this.angle = angle
    repaint
  }


  def setApproxPoints(rdd: RDD[Vector]) {
    approxPoints = rdd.collect
    repaint
  }

  def plotDot(g: Graphics, x: Int, y: Int) {
    val r = 5
    val noSamp = 6*r
    var x1 = x
    var y1 = y + r
    for( j <- 1 to noSamp ) {
      val x2 = (x.toDouble + math.sin( j.toDouble*2*math.Pi/noSamp )*r + .5).toInt
      val y2 = (y.toDouble + math.cos( j.toDouble*2*math.Pi/noSamp )*r + .5).toInt
      g.drawLine(x1, ht - y1, x2, ht - y2)
      x1 = x2
      y1 = y2
    }
  }

  def plotLine(g: Graphics, x1: Int, y1: Int, x2: Int, y2: Int) {
    g.drawLine(x1, ht - y1, x2, ht - y2)
  }

  def calcCord(arr: Array[Double], angle: Double):
      (Double, Double, Double, Double, Double, Double) = {

    var arrOut = new Array[Double](6)

    val x = arr(0)*math.cos(angle) - arr(1)*math.sin(angle)
    val y = arr(0)*math.sin(angle) + arr(1)*math.cos(angle)
    val z = arr(2)

    val x0 = arr(0)*math.cos(angle) - arr(1)*math.sin(angle)
    val y0 = arr(0)*math.sin(angle) + arr(1)*math.cos(angle)
    val z0 = 0

    val xs = (arr(0) + shadowFrac*arr(2))*math.cos(angle) - arr(1)*math.sin(angle)
    val ys = (arr(0) + shadowFrac*arr(2))*math.sin(angle) + arr(1)*math.cos(angle)
    val zs = 0

    arrOut(0) = y - .5*x
    arrOut(1) = z - .25*x

    arrOut(2) = y0 - .5*x0
    arrOut(3) = z0 - .25*x0

    arrOut(4) = ys - .5*xs
    arrOut(5) = zs - .25*xs

    (arrOut(0), arrOut(1), arrOut(2), arrOut(3), arrOut(4), arrOut(5))

  }

  override def paint(g: Graphics) = {

    if(points!=null) {

      var p = points.map(T => calcCord(T.toArray, angle)).toArray

      var xmax = p(0)._1
      var xmin = p(0)._1
      var ymax = p(0)._2
      var ymin = p(0)._2

      for(i <- 0 to p.size-1) {

        if(xmax<p(i)._1) {
          xmax = p(i)._1
        }
        if(xmax<p(i)._3) {
          xmax = p(i)._3
        }
        if(xmax<p(i)._5) {
          xmax = p(i)._5
        }

        if(xmin>p(i)._1) {
          xmin = p(i)._1
        }
        if(xmin>p(i)._3) {
          xmin = p(i)._3
        }
        if(xmin>p(i)._5) {
          xmin = p(i)._5
        }

        if(ymax<p(i)._2) {
          ymax = p(i)._2
        }
        if(ymax<p(i)._4) {
          ymax = p(i)._4
        }
        if(ymax<p(i)._6) {
          ymax = p(i)._6
        }

        if(ymin>p(i)._2) {
          ymin = p(i)._2
        }
        if(ymin>p(i)._4) {
          ymin = p(i)._4
        }
        if(ymin>p(i)._6) {
          ymin = p(i)._6
        }

      }

      for(i <- 0 to p.size-1) {

        var x_ = (((p(i)._1 - xmin)/(xmax - xmin))*(wd - 40) + 20.5).toInt
        var y_ = (((p(i)._2 - ymin)/(ymax - ymin))*(ht - 40) + 20.5).toInt
        var x0 = (((p(i)._3 - xmin)/(xmax - xmin))*(wd - 40) + 20.5).toInt
        var y0 = (((p(i)._4 - ymin)/(ymax - ymin))*(ht - 40) + 20.5).toInt
        var xs = (((p(i)._5 - xmin)/(xmax - xmin))*(wd - 40) + 20.5).toInt
        var ys = (((p(i)._6 - ymin)/(ymax - ymin))*(ht - 40) + 20.5).toInt

        g.setColor(Color.black)
        plotDot(g, x_, y_)
        plotLine(g, x_, y_, x0, y0)
        g.setColor(Color.gray)
        plotLine(g, x0, y0, xs, ys)

      }

      if(approxPoints != null) {

        var p = approxPoints.map(T => calcCord(T.toArray, angle))

        for(i <- 0 to p.size-1) {

          var x_ = (((p(i)._1 - xmin)/(xmax - xmin))*(wd - 40) + 20.5).toInt
          var y_ = (((p(i)._2 - ymin)/(ymax - ymin))*(ht - 40) + 20.5).toInt
          var x0 = (((p(i)._3 - xmin)/(xmax - xmin))*(wd - 40) + 20.5).toInt
          var y0 = (((p(i)._4 - ymin)/(ymax - ymin))*(ht - 40) + 20.5).toInt
          var xs = (((p(i)._5 - xmin)/(xmax - xmin))*(wd - 40) + 20.5).toInt
          var ys = (((p(i)._6 - ymin)/(ymax - ymin))*(ht - 40) + 20.5).toInt

          g.setColor(Color.red)
          plotDot(g, x_, y_)
          plotLine(g, x_, y_, x0, y0)
          g.setColor(Color.magenta)
          plotLine(g, x0, y0, xs, ys)

        }

      }

    }
  }
}

class OutputFrame3D(title: String, shadowFrac: Double) extends Frame(title) {

  val wd = 800
  val ht = 600

  def this(title: String) = this(title, .25)

  var outputCanvas = new OutputCanvas3D(wd, ht, shadowFrac)

  def apply() {
    addWindowListener(windowAdapter3D)
    setSize(wd, ht)
    add("Center", outputCanvas)
    show()
  }

  def setData(rdd: RDD[Vector]) {
    outputCanvas.setData(rdd)
  }

  def setAngle(angle: Double) {
    outputCanvas.setAngle(angle)
  }

  def setApproxPoints(rdd: RDD[Vector]) {
    outputCanvas.setApproxPoints(rdd)
  }

}

object ANNDemo {

  var rand = new Random(0)

  def generateInput2D(f: Double => Double, xmin: Double, xmax: Double, noPoints: Int):
      Array[(Vector,Vector)] =
  {

    var out = new Array[(Vector,Vector)](noPoints)

    for(i <- 0 to noPoints - 1) {
      val x = xmin + rand.nextDouble()*(xmax - xmin)
      val y = f(x)
      out(i) = (Vectors.dense(x), Vectors.dense(y))
    }

    return out

  }


  def generateInput3D(f: (Double,Double) => Double, xmin: Double, xmax: Double,
      ymin: Double, ymax: Double, noPoints: Int): Array[(Vector,Vector)] = {

    var out = new Array[(Vector,Vector)](noPoints)

    for(i <- 0 to noPoints - 1) {

      val x = xmin + rand.nextDouble()*(xmax - xmin)
      val y = ymin + rand.nextDouble()*(ymax - ymin)
      val z = f(x, y)

      var arr = new Array[Double](2)

      arr(0) = x
      arr(1) = y
      out(i) = (Vectors.dense(arr), Vectors.dense(z))

    }

    out

  }

  def generateInput4D(f: Double => (Double,Double,Double),
      tmin: Double, tmax: Double, noPoints: Int): Array[(Vector,Vector)] = {

    var out = new Array[(Vector,Vector)](noPoints)

    for(i <- 0 to noPoints - 1) {

      val t: Double = tmin + rand.nextDouble()*(tmax - tmin)
      var arr = new Array[Double](3)
      var F = f(t)

      arr(0) = F._1
      arr(1) = F._2
      arr(2) = F._3

      out(i) = (Vectors.dense(t), Vectors.dense(arr))
    }

    out

  }

  def f( T: Double ): Double = {
    val y = 0.5 + Math.abs(T/5).toInt.toDouble*.15 + math.sin(T*math.Pi/10)*.1
    assert(y <= 1)
    y
  }

  def f3D(x: Double, y: Double): Double = {
    .5 + .24*Math.sin(x*2*math.Pi/10) + .24*Math.cos(y*2*math.Pi/10)
  }

  def f4D(t: Double): (Double, Double,Double) = {
    val x = Math.abs(.8*Math.cos(t*2*math.Pi/20)) + .1
    val y = (11 + t)/22
    val z = .5 + .35*Math.sin(t*2*math.Pi/5)*Math.cos( t*2*math.Pi/10 ) + .15*t/11
    (x, y, z)
  }

  def concat(v1: Vector, v2: Vector): Vector = {

    var a1 = v1.toArray
    var a2 = v2.toArray
    var a3 = new Array[Double](a1.size + a2.size)

    for(i <- 0 to a1.size - 1) {
      a3(i) = a1(i)
    }

    for(i <- 0 to a2.size - 1) {
      a3(i + a1.size) = a2(i)
    }

    Vectors.dense(a3)

  }

  def main(arg: Array[String]) {

    println("ANN demo")
    println

    val formatter = new SimpleDateFormat("hh:mm:ss")

    var curAngle: Double = 0.0

    var outputFrame2D: OutputFrame2D = null
    var outputFrame3D: OutputFrame3D = null
    var outputFrame4D: OutputFrame3D = null

    outputFrame2D = new OutputFrame2D("x -> y")
    outputFrame2D.apply

    outputFrame3D = new OutputFrame3D("(x,y) -> z", 1)
    outputFrame3D.apply

    outputFrame4D = new OutputFrame3D("t -> (x,y,z)")
    outputFrame4D.apply

    var A = 20.0
    var B = 50.0

    var conf = new SparkConf().setAppName("Parallel ANN").setMaster("local[1]")
    var sc = new SparkContext(conf)

    val testRDD2D =
      sc.parallelize(generateInput2D( T => f(T), -10, 10, 100 ), 2).cache
    val testRDD3D =
      sc.parallelize(generateInput3D((x,y) => f3D(x,y), -10, 10, -10, 10, 200 ), 2).cache
    val testRDD4D =
      sc.parallelize( generateInput4D( t => f4D(t), -10, 10, 100 ), 2 ).cache

    val validationRDD2D =
      sc.parallelize(generateInput2D( T => f(T), -10, 10, 100 ), 2).cache
    val validationRDD3D =
      sc.parallelize(generateInput3D( (x,y) => f3D(x,y), -10, 10, -10, 10, 100 ), 2).cache
    val validationRDD4D =
      sc.parallelize( generateInput4D( t => f4D(t), -10, 10, 100 ), 2 ).cache

    outputFrame2D.setData( testRDD2D.map( T => concat( T._1, T._2 ) ) )
    outputFrame3D.setData( testRDD3D.map( T => concat( T._1, T._2 ) ) )
    outputFrame4D.setData( testRDD4D.map( T => T._2 ) )

    var starttime = Calendar.getInstance().getTime()
    println("Training 2D")
    var model2D = ArtificialNeuralNetwork.train(testRDD2D, Array[Int](5, 3), 1000, 1e-8)
    var stoptime = Calendar.getInstance().getTime()
    println(((stoptime.getTime-starttime.getTime + 500) / 1000) + "s")

    starttime = stoptime
    println("Training 3D")
    var model3D = ArtificialNeuralNetwork.train(testRDD3D, Array[Int](20), 1000, 1e-8)
    stoptime = Calendar.getInstance().getTime()
    println(((stoptime.getTime-starttime.getTime + 500) / 1000) + "s")

    starttime = stoptime
    println("Training 4D")
    var model4D = ArtificialNeuralNetwork.train(testRDD4D, Array[Int](20), 1000, 1e-8)
    stoptime = Calendar.getInstance().getTime()
    println(((stoptime.getTime-starttime.getTime + 500) / 1000) + "s")

    val predictedAndTarget2D = validationRDD2D.map(T => (T._1, T._2, model2D.predict(T._1)))
    val predictedAndTarget3D = validationRDD3D.map(T => (T._1, T._2, model3D.predict(T._1)))
    val predictedAndTarget4D = validationRDD4D.map(T => (T._1, T._2, model4D.predict(T._1)))

    var err2D = predictedAndTarget2D.map( T =>
      (T._3.toArray(0) - T._2.toArray(0))*(T._3.toArray(0) - T._2.toArray(0))
    ).reduce((u,v) => u + v)

    var err3D = predictedAndTarget3D.map( T =>
      (T._3.toArray(0) - T._2.toArray(0))*(T._3.toArray(0) - T._2.toArray(0))
    ).reduce((u,v) => u + v)

    var err4D = predictedAndTarget4D.map(T => {

      val v1 = T._2.toArray
      val v2 = T._3.toArray

      (v1(0) - v2(0)) * (v1(0) - v2(0)) +
      (v1(1) - v2(1)) * (v1(1) - v2(1)) +
      (v1(2) - v2(2)) * (v1(2) - v2(2))

    }).reduce((u,v) => u + v)

    println("Error 2D/3D/4D: " + (err2D, err3D, err4D))

    val predicted2D = predictedAndTarget2D.map(
      T => concat(T._1, T._3)
    )

    val predicted3D = predictedAndTarget3D.map(
      T => concat(T._1, T._3)
    )

    val predicted4D = predictedAndTarget4D.map(
      T => T._3
    )

    outputFrame2D.setApproxPoints(predicted2D)
    outputFrame3D.setApproxPoints(predicted3D)
    outputFrame4D.setApproxPoints(predicted4D)

    while(true) { // stops when closing the window

      curAngle = curAngle + math.Pi/8
      if(curAngle >= 2*math.Pi) {
        curAngle = curAngle - 2*math.Pi
      }

      outputFrame3D.setAngle(curAngle)
      outputFrame4D.setAngle(curAngle)

      outputFrame3D.repaint
      outputFrame4D.repaint

      Thread.sleep(3000)

    }

    sc.stop

  }

}
