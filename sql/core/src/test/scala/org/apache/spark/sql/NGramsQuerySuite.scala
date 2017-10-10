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

package org.apache.spark.sql

import scala.collection.mutable

import org.apache.spark.sql.test.SharedSQLContext

class NGramsQuerySuite extends QueryTest with SharedSQLContext {

  import testImplicits._

  val single_row = "ngrams_single_row"
  val multiple_rows = "ngrams_multiple_rows"
  val array_of_array = "ngrams_array_of_array"

  val abc = "abc"
  val bcd = "bcd"

  val pattern_tiny1 = Array(abc, abc)
  val pattern_tiny2 = Array(abc, bcd)
  val pattern_tiny3 = Array(bcd, abc)
  val pattern_tiny4 = Array(bcd, bcd)

  val pattern1 = Array[String](abc, abc, bcd, abc, bcd)
  val pattern2 = Array[String](bcd, abc, abc, abc, abc, bcd)

  def wrappedMap(tuple2: Tuple2[mutable.WrappedArray[AnyRef], Double]):
    Map[mutable.WrappedArray[AnyRef], Double] = {
    Map[mutable.WrappedArray[AnyRef], Double](tuple2)
  }

  def wrappedArray(array: Array[String]): mutable.WrappedArray[AnyRef] = {
    mutable.WrappedArray.make[AnyRef](array)
  }

  val expected1 = Row(Array(
    wrappedMap((wrappedArray(pattern_tiny2), 2.0)),
    wrappedMap((wrappedArray(pattern_tiny1), 1.0)),
    wrappedMap((wrappedArray(pattern_tiny3), 1.0))
  ))

  val expected2 = Row(Array(
    wrappedMap((wrappedArray(pattern_tiny1), 4.0)),
    wrappedMap((wrappedArray(pattern_tiny2), 3.0)),
    wrappedMap((wrappedArray(pattern_tiny3), 2.0))
  ))

  val expected3 = Row(Array(
    wrappedMap((wrappedArray(pattern_tiny2), 2.0)),
    wrappedMap((wrappedArray(pattern_tiny3), 1.0)),
    wrappedMap((wrappedArray(pattern_tiny4), 1.0))
  ))

  test(single_row) {
      checkAnswer(
        spark.sql(
          s"""
             |SELECT
             |  ngrams(array('abc', 'abc', 'bcd', 'abc', 'bcd'), 2, 4)
           """.stripMargin),
        expected1
      )
  }

  test(multiple_rows) {
    withTable(multiple_rows) {
      List[Array[String]](pattern1, pattern2).toDF("col").createOrReplaceTempView(multiple_rows)
      checkAnswer(
        spark.sql(
          s"""
            |SELECT
            |   ngrams(col, 2, 4)
            |FROM $multiple_rows
           """.stripMargin),
        expected2
      )
    }
  }

  test(array_of_array) {
    checkAnswer(
      spark.sql(
        s"""
           |SELECT
           |  ngrams(array(
           |    array('abc', 'bcd', 'bcd'),
           |    array('abc', 'bcd'),
           |    array('abc'),
           |    array('bcd', 'abc')),
           |    2,
           |    4)
           """.stripMargin),
      expected3
    )
  }
}
