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

package org.apache.spark.sql.execution

import org.apache.spark.{SparkEnv, SparkException, SparkUnsupportedOperationException}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.Deduplicate
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch

class SparkPlanSuite extends QueryTest with SharedSparkSession {

  test("SPARK-21619 execution of a canonicalized plan should fail") {
    val plan = spark.range(10).queryExecution.executedPlan.canonicalized

    intercept[SparkException] { plan.execute() }
    intercept[SparkException] { plan.executeCollect() }
    intercept[SparkException] { plan.executeCollectPublic() }
    intercept[SparkException] { plan.executeToIterator() }
    intercept[SparkException] { plan.executeBroadcast() }
    intercept[SparkException] { plan.executeTake(1) }
    intercept[SparkException] { plan.executeTail(1) }
  }

  test("SPARK-23731 plans should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val fileSourceScanExec =
          df.queryExecution.sparkPlan.collectFirst { case p: FileSourceScanExec => p }.get
        val serializer = SparkEnv.get.serializer.newInstance()
        val readback =
          serializer.deserialize[FileSourceScanExec](serializer.serialize(fileSourceScanExec))
        try {
          readback.canonicalized
        } catch {
          case e: Throwable => fail("FileSourceScanExec was not canonicalizable", e)
        }
      }
    }
  }

  test("SPARK-27418 BatchScanExec should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val batchScanExec =
          df.queryExecution.sparkPlan.collectFirst { case p: BatchScanExec => p }.get
        val serializer = SparkEnv.get.serializer.newInstance()
        val readback =
          serializer.deserialize[BatchScanExec](serializer.serialize(batchScanExec))
        try {
          readback.canonicalized
        } catch {
          case e: Throwable => fail("BatchScanExec was not canonicalizable", e)
        }
      }
    }
  }

  test("SPARK-25357 SparkPlanInfo of FileScan contains nonEmpty metadata") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(5).write.parquet(path.getAbsolutePath)
        val f = spark.read.parquet(path.getAbsolutePath)
        assert(SparkPlanInfo.fromSparkPlan(f.queryExecution.sparkPlan).metadata.nonEmpty)
      }
    }
  }

  test("SPARK-30780 empty LocalTableScan should use RDD without partitions") {
    assert(LocalTableScanExec(Nil, Nil).execute().getNumPartitions == 0)
  }

  test("SPARK-33617: change default parallelism of LocalTableScan") {
    Seq(1, 4).foreach { minPartitionNum =>
      withSQLConf(SQLConf.LEAF_NODE_DEFAULT_PARALLELISM.key -> minPartitionNum.toString) {
        val df = spark.sql("SELECT * FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8)")
        assert(df.rdd.partitions.length === minPartitionNum)
      }
    }
  }

  test("SPARK-34420: Throw exception if non-streaming Deduplicate is not replaced by aggregate") {
    val df = spark.range(10)
    val planner = spark.sessionState.planner
    val deduplicate = Deduplicate(df.queryExecution.analyzed.output, df.queryExecution.analyzed)
    checkError(
      exception = intercept[SparkException] {
        planner.plan(deduplicate)
      },
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" -> ("Deduplicate operator for non streaming data source should have been " +
          "replaced by aggregate in the optimizer")))
  }

  test("SPARK-37221: The collect-like API in SparkPlan should support columnar output") {
    val emptyResults = ColumnarOp(LocalTableScanExec(Nil, Nil)).toRowBased.executeCollect()
    assert(emptyResults.isEmpty)

    val relation = LocalTableScanExec(
      Seq(AttributeReference("val", IntegerType)()), Seq(InternalRow(1)))
    val nonEmpty = ColumnarOp(relation).toRowBased.executeCollect()
    assert(nonEmpty === relation.executeCollect())
  }

  test("SPARK-37779: ColumnarToRowExec should be canonicalizable after being (de)serialized") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      withTempPath { path =>
        spark.range(1).write.parquet(path.getAbsolutePath)
        val df = spark.read.parquet(path.getAbsolutePath)
        val columnarToRowExec =
          df.queryExecution.executedPlan.collectFirst { case p: ColumnarToRowExec => p }.get
        try {
          spark.range(1).foreach { _ =>
            columnarToRowExec.canonicalized
            ()
          }
        } catch {
          case e: Throwable => fail("ColumnarToRowExec was not canonicalizable", e)
        }
      }
    }
  }
}

case class ColumnarOp(child: SparkPlan) extends UnaryExecNode {
  override val supportsColumnar: Boolean = true
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] =
    RowToColumnarExec(child).executeColumnar()
  override protected def doExecute(): RDD[InternalRow] = throw SparkUnsupportedOperationException()
  override def output: Seq[Attribute] = child.output
  override protected def withNewChildInternal(newChild: SparkPlan): ColumnarOp =
    copy(child = newChild)
}
