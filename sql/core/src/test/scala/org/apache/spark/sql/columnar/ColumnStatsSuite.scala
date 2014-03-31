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

package org.apache.spark.sql.columnar

import scala.reflect.ClassTag
import scala.util.Random

import org.scalatest.FunSuite

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types._

class ColumnStatsSuite extends FunSuite {
  testColumnStats[BooleanType.type, BooleanColumnStats] {
    Random.nextBoolean()
  }

  testColumnStats[IntegerType.type, IntColumnStats] {
    Random.nextInt()
  }

  testColumnStats[LongType.type, LongColumnStats] {
    Random.nextLong()
  }

  testColumnStats[ShortType.type, ShortColumnStats] {
    (Random.nextInt(Short.MaxValue * 2) - Short.MaxValue).toShort
  }

  testColumnStats[ByteType.type, ByteColumnStats] {
    (Random.nextInt(Byte.MaxValue * 2) - Byte.MaxValue).toByte
  }

  testColumnStats[DoubleType.type, DoubleColumnStats] {
    Random.nextDouble()
  }

  testColumnStats[FloatType.type, FloatColumnStats] {
    Random.nextFloat()
  }

  testColumnStats[StringType.type, StringColumnStats] {
    Random.nextString(Random.nextInt(32))
  }

  def testColumnStats[T <: NativeType, U <: NativeColumnStats[T]: ClassTag](
      mkRandomValue: => U#JvmType) {

    val columnStatsClass = implicitly[ClassTag[U]].runtimeClass
    val columnStatsName = columnStatsClass.getSimpleName

    test(s"$columnStatsName: empty") {
      val columnStats = columnStatsClass.newInstance().asInstanceOf[U]
      assert((columnStats.lowerBound, columnStats.upperBound) === columnStats.initialBounds)
    }

    test(s"$columnStatsName: non-empty") {
      val columnStats = columnStatsClass.newInstance().asInstanceOf[U]
      val values = Seq.fill[U#JvmType](10)(mkRandomValue)
      val row = new GenericMutableRow(1)

      values.foreach { value =>
        row(0) = value
        columnStats.gatherStats(row, 0)
      }

      assert(columnStats.lowerBound === values.min(columnStats.ordering))
      assert(columnStats.upperBound === values.max(columnStats.ordering))
    }
  }
}
