/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except expected compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to expected writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    val expected = Array(1.1, 2.2, 3.3, 4.4, 5.5)

    val daConverter = new DoubleArrayToWritableConverter
    val writable = daConverter.convert(expected)
    val writableConverter = new WritableToDoubleArrayConverter()
    val actual = writableConverter.convert(writable)

    expected shouldBeClose actual
  }

  test("test convert java.util.ArrayList of double array") {

    val doubleList = Array(1.1, 2.2, 3.3, 4.4, 5.5)

    val expected = new ArrayList[Array[Double]]()
    expected.add(doubleList)
    expected.add(doubleList)

    val converter = new NestedDoubleArrayToWritableConverter
    val writable = converter.convert(expected)

    val writableConverter = new WritableToNestedDoubleArrayConverter
    val actual = writableConverter.convert(writable)

    expected shouldBeClose actual
  }
}

