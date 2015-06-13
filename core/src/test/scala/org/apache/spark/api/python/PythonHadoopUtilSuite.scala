package org.apache.spark.api.python

import java.util.ArrayList

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.JavaConversions
import scala.language.implicitConversions

class ShouldBeClose private (lhs: Either[Either[Array[Double], ArrayList[_]], Double]) {
  val precision = 1e-7

  def shouldBeClose(rhs: Double) = {
    val lhsVal = lhs.right.get
    rhs +- precision == lhsVal +- precision
  }

  def shouldBeClose(rhs: Array[Double]) : Unit = {
    val lhsVal = lhs.left.get.left.get
    lhsVal.size shouldBe rhs.size
    (lhsVal.toSeq zip rhs.toSeq).map(e => ShouldBeClose(e._1).shouldBeClose(e._2)).reduceLeft(_ == _) shouldBe true
  }

  def shouldBeClose(rhs: ArrayList[_]) : Unit = {
    val lhsVal = lhs.left.get.right.get
    lhsVal.size shouldBe rhs.size
    (JavaConversions.asScalaBuffer(lhsVal).toSeq zip JavaConversions.asScalaBuffer(rhs).toSeq).map(e => {
      val lhsInner = e._1.asInstanceOf[Array[Double]]
      val rhsInner = e._2.asInstanceOf[Array[Double]]
      ShouldBeClose(lhsInner).shouldBeClose(rhsInner)
    })
  }
}

object ShouldBeClose {
   def apply(arg:Array[Double]) = new ShouldBeClose(Left(Left(arg)))
   def apply(arg:ArrayList[_]) = new ShouldBeClose(Left(Right(arg)))
   def apply(arg:Double) = new ShouldBeClose(Right(arg))
}

class PythonHadoopUtilSuite extends FunSuite {

  implicit def arrayDouble2ArrayDouble(i: Array[Double]) = ShouldBeClose(i)
  implicit def arrayDouble2ArrayDouble(i: ArrayList[_]) = ShouldBeClose(i)


  test("test convert array of doubles") {

    val in = Array(1.1, 2.2, 3.3, 4.4, 5.5)

    val daConverter = new DoubleArrayToWritableConverter
    val writable = daConverter.convert(in)
    val writableConverter = new WritableToDoubleArrayConverter()
    val out = writableConverter.convert(writable)

    in shouldBeClose out
  }

  test("test convert java.util.ArrayList of double array") {

    val doubleList = Array(1.1, 2.2, 3.3, 4.4, 5.5)

    val in = new ArrayList[Array[Double]]()
    in.add(doubleList)
    in.add(doubleList)

    val converter = new DoubleArrayListOfDoubleArrayToWritableConverter
    val writable = converter.convert(in)

    val writableConverter = new WritableToDoubleArrayListOfDoubleArrayConverter
    val out = writableConverter.convert(writable)

    in shouldBeClose out
  }
}
