package spark.mllib.classification

import scala.util.Random
import scala.math.signum

import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import spark.SparkContext
import spark.SparkContext._

import java.io._

class SVMSuite extends FunSuite with BeforeAndAfterAll {
  val sc = new SparkContext("local", "test")

  override def afterAll() {
    sc.stop()
    System.clearProperty("spark.driver.port")
  }

  test("SVM_LocalRandomSGD") {
    val nPoints = 10000
    val rnd = new Random(42)

    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())
    val x2 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val A = 2.0
    val B = -1.5
    val C = 1.0

    val y = (0 until nPoints).map { i =>
      signum(A + B * x1(i) + C * x2(i) + 0.0*rnd.nextGaussian())
    }

    val testData = (0 until nPoints).map(i => (y(i).toDouble, Array(x1(i),x2(i)))).toArray

    val testRDD = sc.parallelize(testData, 2)
    testRDD.cache()

    val writer_data = new PrintWriter(new File("svmtest.dat"))
    testData.foreach(yx => {
      writer_data.write(yx._1 + "")
      yx._2.foreach(xi => writer_data.write("\t" + xi))
      writer_data.write("\n")})
    writer_data.close()

    val svm = new SVM_LocalRandomSGD().setStepSize(1.0)
                      .setRegParam(1.0)
                      .setNumIterations(100)

    val model = svm.train(testRDD)

    val yPredict = (0 until nPoints).map(i => model.predict(Array(x1(i),x2(i))))

    val accuracy = ((y zip yPredict).map(yy => if (yy._1==yy._2) 1 else 0).reduceLeft(_+_).toDouble / nPoints.toDouble)

    assert(accuracy >= 0.90, "Accuracy (" + accuracy + ") too low")
  }
}
