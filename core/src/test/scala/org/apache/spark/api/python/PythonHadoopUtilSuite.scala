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

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.language.implicitConversions

class ShouldBeClose private (lhs: Either[Array[Double], Double]) {
  val precision = 1e-7

  def shouldBeClose(rhs: Double) = {
    val lhsVal = lhs.right.get
    rhs +- precision == lhsVal +- precision
  }

  def shouldBeClose(rhs: Array[Double]) : Unit = {
    val lhsVal = lhs.left.get
    lhsVal.size shouldBe rhs.size
    (lhsVal.toSeq zip rhs.toSeq).map(e => ShouldBeClose(e._1).shouldBeClose(e._2)).reduceLeft(_ == _) shouldBe true
  }
}

object ShouldBeClose {
   def apply(arg:Array[Double]) = new ShouldBeClose(Left(arg))
   def apply(arg:Double) = new ShouldBeClose(Right(arg))
}

class PythonHadoopUtilSuite extends FunSuite {

  implicit def arrayDouble2ArrayDouble(i: Array[Double]) = ShouldBeClose(i)

  test("test convert array of doubles") {

    val expected = Array(1.1, 2.2, 3.3, 4.4, 5.5)

    val daConverter = new DoubleArrayToWritableConverter
    val writable = daConverter.convert(expected)
    val writableConverter = new WritableToDoubleArrayConverter()
    val actual = writableConverter.convert(writable)

    expected shouldBeClose actual
  }

  test("test convert Array of Bytes to byte array") {

    val expected = Array(1.toByte, 2.toByte, 3.toByte, 5.toByte, 8.toByte, 13.toByte)

    val converter = new ByteArrayToWritableConverter
    val writable = converter.convert(expected)
    val writableConverter = new WritableToByteArrayConverter
    val actual = writableConverter.convert(writable)

    expected shouldEqual actual
  }
}
