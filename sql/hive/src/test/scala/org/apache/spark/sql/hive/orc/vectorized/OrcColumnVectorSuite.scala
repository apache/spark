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

package org.apache.spark.sql.hive.orc.vectorized

import scala.util.Random

import org.apache.commons.lang.NotImplementedException
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.hadoop.hive.ql.io.orc.OrcColumnVector
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.RandomDataGenerator
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class OrcColumnVectorSuite extends SparkFunSuite {
  // This helper method access the internal vector of Hive's ColumnVector classes.
  private def fillColumnVector[T](col: ColumnVector, values: Seq[T]): Unit = {
    col match {
      case lv: LongColumnVector =>
        assert(lv.vector.length == values.length)
        values.zipWithIndex.map { case (v, idx) =>
          lv.vector(idx) = v.asInstanceOf[Long]
        }
      case bv: BytesColumnVector =>
        assert(bv.vector.length == values.length)
        values.zipWithIndex.map { case (v, idx) =>
          val array = v.asInstanceOf[Seq[Byte]].toArray
          bv.vector(idx) = array
          bv.start(idx) = 0
          bv.length(idx) = array.length
        }
      case dv: DoubleColumnVector =>
        assert(dv.vector.length == values.length)
        values.zipWithIndex.map { case (v, idx) =>
          dv.vector(idx) = v.asInstanceOf[Double]
        }
      case dv: DecimalColumnVector =>
        assert(dv.vector.length == values.length)
        values.zipWithIndex.map { case (v, idx) =>
          val writable = new HiveDecimalWritable(v.asInstanceOf[HiveDecimal])
          dv.vector(idx) = writable
        }
      case _ =>
        assert(false, s"${col.getClass.getName} is not supported")
    }
  }

  private def dataGenerator[T](num: Int)(randomize: (Int) => T): Seq[T] = {
    (0 until num).map { i =>
      randomize(i)
    }
  }

  private def getAllRowsFromColumn[T]
      (rowNum: Int, col: OrcColumnVector)(accessor: (OrcColumnVector, Int) => T): Seq[T] = {
    (0 until rowNum).map { rowId =>
      accessor(col, rowId)
    }
  }

  private def testLongColumnVector[T](num: Int, dt: DataType)
      (genExpected: (Seq[Long] => Seq[T]))
      (genActual: (OrcColumnVector, Int) => Seq[T]): Unit = {
    val seed = System.currentTimeMillis()
    val random = new Random(seed)

    val data = dataGenerator(num) { _ =>
      random.nextLong()
    }

    val lv = new LongColumnVector(num)
    fillColumnVector(lv, data)
    assert(data === lv.vector)

    val expected = genExpected(data)

    val orcCol = new OrcColumnVector(lv, dt)
    val actual = genActual(orcCol, num)
    assert(actual === expected)
  }

  private def testDoubleColumnVector[T](num: Int, dt: DataType)
      (genExpected: (Seq[Double] => Seq[T]))
      (genActual: (OrcColumnVector, Int) => Seq[T]): Unit = {
    val seed = System.currentTimeMillis()
    val random = new Random(seed)

    val data = dataGenerator(num) { _ =>
      random.nextDouble()
    }

    val lv = new DoubleColumnVector(num)
    fillColumnVector(lv, data)
    assert(data === lv.vector)

    val expected = genExpected(data)

    val orcCol = new OrcColumnVector(lv, dt)
    val actual = genActual(orcCol, num)
    assert(actual === expected)
  }

  private def testBytesColumnVector[T](num: Int, dt: DataType)
      (genExpected: (Seq[Seq[Byte]] => Seq[T]))
      (genActual: (OrcColumnVector, Int) => Seq[T]): Unit = {
    val seed = System.currentTimeMillis()
    val random = new Random(seed)

    val schema = new StructType().add("binary", BinaryType, false)
    val data = dataGenerator(num) { _ =>
      RandomDataGenerator.randomRow(random, schema).getAs[Array[Byte]](0).toSeq
    }

    val lv = new BytesColumnVector(num)
    fillColumnVector(lv, data)
    assert(data === lv.vector)

    val expected = genExpected(data)

    val orcCol = new OrcColumnVector(lv, dt)
    val actual = genActual(orcCol, num)
    actual.zip(expected).foreach { case (a, e) =>
      assert(a === e)
    }
  }

  private def testDecimalColumnVector(num: Int)
      (genExpected: (Seq[HiveDecimal] => Seq[java.math.BigDecimal]))
      (genActual: (OrcColumnVector, Int, Int, Int) => Seq[java.math.BigDecimal]): Unit = {
    val seed = System.currentTimeMillis()
    val random = new Random(seed)

    val decimalTypes = Seq(DecimalType.ShortDecimal, DecimalType.IntDecimal,
      DecimalType.ByteDecimal, DecimalType.FloatDecimal, DecimalType.LongDecimal)

    decimalTypes.foreach { decimalType =>
      val schema = new StructType().add("decimal", decimalType, false)
      val data = dataGenerator(num) { _ =>
        val javaDecimal = RandomDataGenerator.randomRow(random, schema).getDecimal(0)
        HiveDecimal.create(javaDecimal)
      }

      val lv = new DecimalColumnVector(num, decimalType.precision, decimalType.scale)
      fillColumnVector(lv, data)
      assert(data === lv.vector.map(_.getHiveDecimal(decimalType.precision, decimalType.scale)))

      val expected = genExpected(data)

      val orcCol = new OrcColumnVector(lv, decimalType)
      val actual = genActual(orcCol, num, decimalType.precision, decimalType.scale)
      actual.zip(expected).foreach { case (a, e) =>
        assert(a.compareTo(e) == 0)
      }
    }
  }

  test("Hive LongColumnVector: Boolean") {
    val genExpected = (data: Seq[Long]) => data.map(_ > 0)
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getBoolean(rowId)
      }
    }
    testLongColumnVector(100, BooleanType)(genExpected)(genActual)
  }

  test("Hive LongColumnVector: Int") {
    val genExpected = (data: Seq[Long]) => data.map(_.toInt)
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getInt(rowId)
      }
    }
    testLongColumnVector(100, IntegerType)(genExpected)(genActual)
  }

  test("Hive LongColumnVector: Byte") {
    val genExpected = (data: Seq[Long]) => data.map(_.toByte)
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getByte(rowId)
      }
    }
    testLongColumnVector(100, ByteType)(genExpected)(genActual)
  }

  test("Hive LongColumnVector: Short") {
    val genExpected = (data: Seq[Long]) => data.map(_.toShort)
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getShort(rowId)
      }
    }
    testLongColumnVector(100, ShortType)(genExpected)(genActual)
  }

  test("Hive LongColumnVector: Long") {
    val genExpected = (data: Seq[Long]) => data
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getLong(rowId)
      }
    }
    testLongColumnVector(100, LongType)(genExpected)(genActual)
  }

  test("Hive DoubleColumnVector: Float") {
    val genExpected = (data: Seq[Double]) => data.map(_.toFloat)
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getFloat(rowId)
      }
    }
    testDoubleColumnVector(100, FloatType)(genExpected)(genActual)
  }

  test("Hive DoubleColumnVector: Double") {
    val genExpected = (data: Seq[Double]) => data
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getDouble(rowId)
      }
    }
    testDoubleColumnVector(100, DoubleType)(genExpected)(genActual)
  }

  test("Hive BytesColumnVector: Binary") {
    val genExpected = (data: Seq[Seq[Byte]]) => data
    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getBinary(rowId).toSeq
      }
    }
    testBytesColumnVector(100, BinaryType)(genExpected)(genActual)
  }

  test("Hive BytesColumnVector: String") {
    val genExpected = (data: Seq[Seq[Byte]]) => {
      data.map(bytes => UTF8String.fromBytes(bytes.toArray, 0, bytes.length))
    }

    val genActual = (orcCol: OrcColumnVector, num: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getUTF8String(rowId)
      }
    }
    testBytesColumnVector(100, StringType)(genExpected)(genActual)
  }

  test("Hive DecimalColumnVector") {
    val genExpected = (data: Seq[HiveDecimal]) => data.map(_.bigDecimalValue())
    val genActual = (orcCol: OrcColumnVector, num: Int, precision: Int, scale: Int) => {
      getAllRowsFromColumn(num, orcCol) { (col, rowId) =>
        col.getDecimal(rowId, precision, scale).toJavaBigDecimal
      }
    }
    testDecimalColumnVector(100)(genExpected)(genActual)
  }
}
