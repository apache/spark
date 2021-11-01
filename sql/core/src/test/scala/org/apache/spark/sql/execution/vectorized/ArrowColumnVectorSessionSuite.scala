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

package org.apache.spark.sql.execution.vectorized

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{ColumnarToRowExec, RowToColumnarExec, SparkPlan, SparkPlanTest, UnaryExecNode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

class ArrowColumnVectorSessionSuite extends SparkPlanTest with SharedSparkSession {

  test("use RowToColumnarExec to build ArrowColumnVector") {
    spark.conf.set(SQLConf.COLUMN_VECTOR_ARROW_ENABLED.key, "true")
    val data =
      Seq(
        Row(0, 0.1f, 0.toShort, "a", List[Int](1, 2, 3), Map[Int, String]((1, "a"), (2, "b")),
        Row("Bobby G. can't swim")))
    val schema = StructType(
      Seq(
        StructField("int", IntegerType, true),
        StructField("float", FloatType, true),
        StructField("short", ShortType, true),
        StructField("string", StringType, true),
        StructField("array", ArrayType(IntegerType), true),
        StructField("map", MapType(IntegerType, StringType), true),
        StructField("struct", StructType(Seq(StructField("str", StringType, true))), true)))
    val indexData =
      spark.createDataFrame(spark.sparkContext.parallelize(data, 1), schema)

    case class ValidateExec(child: SparkPlan) extends UnaryExecNode {
      override def supportsColumnar: Boolean = true
      override def output: Seq[Attribute] = child.output
      override protected def doExecute(): RDD[InternalRow] = {
        throw new java.lang.UnsupportedOperationException()
      }
      override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
        copy(child = newChild)

      override def doExecuteColumnar(): RDD[ColumnarBatch] = {
        child.executeColumnar().mapPartitions { iter =>
          iter.map(batch => {
            assert(batch.column(0).isInstanceOf[ArrowColumnVector])
            assert(batch.column(0).getInt(0) == 0)
            assert(batch.column(1).getFloat(0) == 0.1f)
            assert(batch.column(2).getShort(0) == 0)
            assert(batch.column(3).getUTF8String(0).toString == "a")
            val arr1 = batch.column(4).getArray(0)
            assert(arr1.getInt(0) == 1)
            assert(arr1.getInt(1) == 2)
            assert(arr1.getInt(2) == 3)
            val map1 = batch.column(5).getMap(0)
            assert(map1.keyArray().getInt(0) == 1)
            assert(map1.valueArray().getUTF8String(0).toString == "a")
            assert(map1.keyArray().getInt(1) == 2)
            assert(map1.valueArray().getUTF8String(1).toString == "b")
            val struct1 = batch.column(6).getStruct(0)
            assert(struct1.getUTF8String(0).toString == "Bobby G. can't swim")
            batch
          })
        }
      }
    }

    ColumnarToRowExec(ValidateExec(RowToColumnarExec(indexData.queryExecution.executedPlan)))
      .execute()
      .collect()
  }
}
