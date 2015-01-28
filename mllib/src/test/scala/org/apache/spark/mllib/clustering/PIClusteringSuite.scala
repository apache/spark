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

package org.apache.spark.mllib.clustering

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.mllib.clustering.PICLinalg.DMatrix
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.scalatest.FunSuite

import scala.util.Random

class PIClusteringSuite extends FunSuite with LocalSparkContext {

  val logger = Logger.getLogger(getClass.getName)

  import org.apache.spark.mllib.clustering.PIClusteringSuite._

  val PIC = PIClustering
  val LA = PICLinalg
  val RDDLA = RDDLinalg
  val A = Array

  test("concentricCirclesTest") {
    concentricCirclesTest()
  }


  def concentricCirclesTest() = {
    val sigma = 1.0
    val nIterations = 10

    val circleSpecs = Seq(
      // Best results for 30 points
      CircleSpec(Point(0.0, 0.0), 0.03, 0.1, 3),
      CircleSpec(Point(0.0, 0.0), 0.3, 0.03, 12),
      CircleSpec(Point(0.0, 0.0), 1.0, 0.01, 15)
      // Add following to get 100 points
      , CircleSpec(Point(0.0, 0.0), 1.5, 0.005, 30),
      CircleSpec(Point(0.0, 0.0), 2.0, 0.002, 40)
    )

    val nClusters = circleSpecs.size
    val cdata = createConcentricCirclesData(circleSpecs)
    withSpark { sc =>
      val vertices = new Random().shuffle(cdata.map { p =>
        (p.label, new BDV(Array(p.x, p.y)))
      })

      val nVertices = vertices.length
      val (ccenters, estCollected) = PIC.run(sc, vertices, nClusters, nIterations)
      logger.info(s"Cluster centers: ${ccenters.mkString(",")} " +
        s"\nEstimates: ${estCollected.mkString("[", ",", "]")}")
      assert(ccenters.size == circleSpecs.length, "Did not get correct number of centers")

    }
  }

  def join[T <: Comparable[T]](a: Map[T, _], b: Map[T, _]) = {
    (a.toSeq ++ b.toSeq).groupBy(_._1).mapValues(_.map(_._2).toList)
  }

  ignore("irisData") {
    irisData()
  }

  def irisData() = {
    import org.apache.spark.mllib.linalg._

    import scala.io.Source
    val irisRaw = Source.fromFile("data/mllib/iris.data").getLines.map(_.split(","))
    val iter: Iterator[(Array[Double], String)] = irisRaw.map { toks => (toks.slice(0, toks.length - 1).map {
      _.toDouble
    }, toks(toks.length - 1))
    }
    withSpark { sc =>
      val irisRdd = sc.parallelize(iter.toSeq.map { case (vect, label) =>
        (Vectors.dense(vect), label)
      })
      val irisVectorsRdd = irisRdd.map(_._1).cache()
      val irisLabelsRdd = irisRdd.map(_._2)
      val model = KMeans.train(irisVectorsRdd, 3, 10, 1)
      val pred = model.predict(irisVectorsRdd).zip(irisLabelsRdd)
      val predColl = pred.collect
      irisVectorsRdd.unpersist()
      predColl
    }

  }


}

object PIClusteringSuite {
  val logger = Logger.getLogger(getClass.getName)
  val LA = PICLinalg
  val A = Array

  def pdoub(d: Double) = f"$d%1.6f"

  case class Point(label: Long, x: Double, y: Double) {
    def this(x: Double, y: Double) = this(-1L, x, y)

    override def toString() = s"($label, (${pdoub(x)},${pdoub(y)}))"
  }

  object Point {
    def apply(x: Double, y: Double) = new Point(-1L, x, y)
  }

  case class CircleSpec(center: Point, radius: Double, noiseToRadiusRatio: Double,
                        nPoints: Int, uniformDistOnCircle: Boolean = true)

  def createConcentricCirclesData(circleSpecs: Seq[CircleSpec]) = {
    import org.apache.spark.mllib.random.StandardNormalGenerator
    val normalGen = new StandardNormalGenerator
    var idStart = 0
    val circles = for (csp <- circleSpecs) yield {
      idStart += 1000
      val circlePoints = for (thetax <- 0 until csp.nPoints) yield {
        val theta = thetax * 2 * Math.PI / csp.nPoints
        val (x, y) = (csp.radius * Math.cos(theta) * (1 + normalGen.nextValue * csp.noiseToRadiusRatio),
          csp.radius * Math.sin(theta) * (1 + normalGen.nextValue * csp.noiseToRadiusRatio))
        (Point(idStart + thetax, x, y))
      }
      circlePoints
    }
    val points = circles.flatten.sortBy(_.label)
    logger.info(printPoints(points))
    points
  }

  def printPoints(points: Seq[Point]) = {
    points.mkString("[", " , ", "]")
  }

  def createAffinityMatrix() = {
    val dat1 = A(
      A(0.0, 0.4, 0.8, 0.9),
      A(0.4, 0.0, 0.7, 0.5),
      A(0.8, 0.7, 0.0, 0.75),
      A(0.9, 0.5, 0.75, 0.0)
    )

    val aMat = new BDM(dat1.length, dat1.length, dat1.flatten)
    logger.info(s"Input mat: ${LA.printMatrix(dat1.flatten, 4, 4)}")
    val Darrarr = dat1.toArray.zipWithIndex.map { case (dvect, ix) =>
      val sum = dvect.foldLeft(0.0) {
        _ + _
      }
      dvect.zipWithIndex.map { case (d, dx) =>
        if (ix == dx) {
          1.0 / sum
        } else {
          0.0
        }
      }
    }
    val D = new BDM(dat1.length, dat1(0).length, Darrarr.flatten)
    print(s"D =\n ${LA.printMatrix(D.toArray, D.rows, D.cols)}")

    val DxDat1: BDM[Double] = D * aMat
    print(s"D * Dat1 =\n ${LA.printMatrix(DxDat1)}")
    (aMat, DxDat1)
  }

  def saveToMatplotLib(dmat: DMatrix, optLegend: Option[Array[String]], optLabels: Option[Array[Array[String]]]) = {

  }

  def main(args: Array[String]) {
    val pictest = new PIClusteringSuite
    pictest.concentricCirclesTest()
  }
}

/**
 * Provides a method to run tests against a {@link SparkContext} variable that is correctly stopped
 * after each test.
 * TODO: import this from the graphx test cases package i.e. may need update to pom.xml
 */
trait LocalSparkContext {
  /** Runs `f` on a new SparkContext and ensures that it is stopped afterwards. */
  def withSpark[T](f: SparkContext => T) = {
    val conf = new SparkConf()
    GraphXUtils.registerKryoClasses(conf)
    val sc = new SparkContext("local", "test", conf)
    try {
      f(sc)
    } finally {
      sc.stop()
    }
  }
}
