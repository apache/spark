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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpressionEvalHelper, Literal}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


class NGramsSuite extends SparkFunSuite with ExpressionEvalHelper{

  test("NGrams") {
    val abc = UTF8String.fromString("abc")
    val bcd = UTF8String.fromString("bcd")

    val pattern1 = Array[UTF8String](abc, abc, bcd, abc, bcd)
    val pattern2 = Array[UTF8String](bcd, abc, abc, abc, abc, bcd)

    var dataList1 = List[Array[UTF8String]]()
    (0 until 100).foreach {
      _ => dataList1 = pattern1 :: dataList1
    }
    var data1 = dataList1.toArray

    var dataList2 = List[Array[UTF8String]]()
    (0 until 100).foreach {
      _ => dataList2 = pattern2 :: dataList2
    }
    var data2 = dataList2.toArray

    val childExpression = BoundReference(0, ArrayType(StringType), nullable = false)
    val accuracy = 10000
    val frequency1 = (List(abc, abc), 400.0)
    val frequency2 = (List(abc, bcd), 300.0)
    val frequency3 = (List(bcd, abc), 200.0)
    val expectedNGrams = List(frequency1, frequency2, frequency3)
    val agg = new NGrams(childExpression, Literal(2), Literal(3))

    val group1Buffer = agg.createAggregationBuffer()
    data1.foreach((list: Array[UTF8String]) => {
      val input = InternalRow.fromSeq(list)
      agg.update(group1Buffer, input)
    })


    val group2Buffer = agg.createAggregationBuffer()
    data2.foreach((list: Array[UTF8String]) => {
      val input = InternalRow.fromSeq(list)
      agg.update(group2Buffer, input)
    })

    val mergeBuffer = agg.createAggregationBuffer()
    agg.merge(mergeBuffer, group1Buffer)
    agg.merge(mergeBuffer, group2Buffer)

    val res = agg.eval(mergeBuffer)
    assert(res.equals(expectedNGrams))
  }

}
