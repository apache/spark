package org.apache.spark.api.python

import java.util.ArrayList

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.JavaConversions

class ShouldBeClose(lhs: Array[Double]) {
  val precision = 1e-7

  def shouldBe(rhs: Array[Double]) = {

    lhs.size shouldBe rhs.size
    (lhs.toList zip rhs.toList).map(e => e._1 +- precision == e._2 +- precision).reduceLeft(_ == _) shouldBe true
  }
}

class PythonHadoopUtilSuite extends FunSuite {

  implicit def arrayDouble2ArrayDouble(i: Array[Double]) = new ShouldBeClose(i)

    test("test convert array of doubles") {

      val in = Array(1.1, 2.2, 3.3, 4.4, 5.5)

      val daConverter = new DoubleArrayToWritableConverter
      val writable = daConverter.convert(in)
      val writableConverter = new WritableToDoubleArrayConverter()
      val out = writableConverter.convert(writable)

      in shouldBe out
    }


  test("test convert nested of array of doubles") {

    val in = Array(Array(-9.9, -8.8),
      Array(7.7, 6.6, 5.5),
      Array(3.3, 4.4, 5.5, 6.6))

    val converter = new NestedDoubleArrayToWritableConverter
    val writable = converter.convert(in)

    val writableConverter = new WritableToNestedDoubleArrayConverter()
    val out = writableConverter.convert(writable)
    in shouldBe out
  }

  test("test convert java.util.ArrayList") {

    val in = new ArrayList(JavaConversions.mutableSeqAsJavaList(Array(1.1, 2.2, 3.3, 4.4, 5.5)))

    val converter = new DoubleArrayListToWritableConverter
    val writable = converter.convert(in)

    val writableConverter = new WritableToDoubleArrayListConverter()
    val out = writableConverter.convert(writable)
    in shouldBe out
  }

  test("test convert nested java.util.ArrayList of doubles") {

    val doubleList = new ArrayList(JavaConversions.mutableSeqAsJavaList(Array(1.1, 2.2, 3.3, 4.4, 5.5)))

    val in = new ArrayList[ArrayList[Double]]()
    in.add(doubleList)
    in.add(doubleList)

    val converter = new NestedDoubleArrayListToWritableConverter
    val writable = converter.convert(in)

    val writableConverter = new WritableToNestedDoubleArrayListConverter
    val out = writableConverter.convert(writable)

    in shouldBe out
  }
}
