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

package org.apache.spark.sql.internal

import java.util.UUID

import org.scalatest.Assertions._

import org.apache.spark.{SparkFunSuite, SparkNoSuchElementException, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.execution.debug.codegenStringSeq
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.tags.ExtendedSQLTest

@ExtendedSQLTest
class ExecutorSideSQLConfSuite extends SparkFunSuite with SQLTestUtils {
  import testImplicits._

  protected var spark: SparkSession = null

  // Create a new [[SparkSession]] running in local-cluster mode.
  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .master("local-cluster[2,1,1024]")
      .appName("testing")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    try {
      spark.stop()
      spark = null
    } finally {
      super.afterAll()
    }
  }

  override def withSQLConf[T](pairs: (String, String)*)(f: => T): T = {
    pairs.foreach { case (k, v) =>
      SQLConf.get.setConfString(k, v)
    }
    try f finally {
      pairs.foreach { case (k, _) =>
        SQLConf.get.unsetConf(k)
      }
    }
  }

  test("ReadOnlySQLConf is correctly created at the executor side") {
    withSQLConf("spark.sql.x" -> "a") {
      val checks = spark.range(10).mapPartitions { _ =>
        val conf = SQLConf.get
        Iterator(conf.isInstanceOf[ReadOnlySQLConf] && conf.getConfString("spark.sql.x") == "a")
      }.collect()
      assert(checks.forall(_ == true))
    }
  }

  test("case-sensitive config should work for json schema inference") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempPath { path =>
        val pathString = path.getCanonicalPath
        spark.range(10).select($"id".as("ID")).write.json(pathString)
        spark.range(10).write.mode("append").json(pathString)
        assert(spark.read.json(pathString).columns.toSet == Set("id", "ID"))
      }
    }
  }

  test("SPARK-24727 CODEGEN_CACHE_MAX_ENTRIES is correctly referenced at the executor side") {
    withSQLConf(StaticSQLConf.CODEGEN_CACHE_MAX_ENTRIES.key -> "300") {
      val checks = spark.range(10).mapPartitions { _ =>
        val conf = SQLConf.get
        Iterator(conf.isInstanceOf[ReadOnlySQLConf] &&
          conf.getConfString(StaticSQLConf.CODEGEN_CACHE_MAX_ENTRIES.key) == "300")
      }.collect()
      assert(checks.forall(_ == true))
    }
  }

  test("SPARK-22219: refactor to control to generate comment",
    DisableAdaptiveExecution("WSCG rule is applied later in AQE")) {
    Seq(true, false).foreach { flag =>
      withSQLConf(StaticSQLConf.CODEGEN_COMMENTS.key -> flag.toString) {
        // with AQE on, the WholeStageCodegen rule is applied when running QueryStageExec.
        val res = codegenStringSeq(spark.range(10).groupBy(col("id") * 2).count()
          .queryExecution.executedPlan)
        assert(res.length == 2)
        assert(res.forall { case (_, code, _) =>
          (code.contains("* Codegened pipeline") == flag) &&
            (code.contains("// input[") == flag)
        })
      }
    }
  }

  test("SPARK-28939: propagate SQLConf also in conversions to RDD") {
    val confs = Seq("spark.sql.a" -> "x", "spark.sql.b" -> "y")
    val physicalPlan = SQLConfAssertPlan(confs)
    val dummyQueryExecution = FakeQueryExecution(spark, physicalPlan)
    withSQLConf(confs: _*) {
      // Force RDD evaluation to trigger asserts
      dummyQueryExecution.toRdd.collect()
    }
    val dummyQueryExecution1 = FakeQueryExecution(spark, physicalPlan)
    // Without setting the configs assertions fail
    intercept[SparkNoSuchElementException](dummyQueryExecution1.toRdd.collect())
  }

  test("SPARK-30556 propagate local properties to subquery execution thread") {
    withSQLConf(StaticSQLConf.SUBQUERY_MAX_THREAD_THRESHOLD.key -> "1") {
      withTempView("l", "m", "n") {
        Seq(true).toDF().createOrReplaceTempView("l")
        val confKey = "spark.sql.y"

        def createDataframe(confKey: String, confValue: String): Dataset[Boolean] = {
          Seq(true)
            .toDF()
            .mapPartitions { _ =>
              if (TaskContext.get().getLocalProperty(confKey) == confValue) {
                Iterator(true)
              } else {
                Iterator.empty
              }
            }
        }

        // set local configuration and assert
        val confValue1 = UUID.randomUUID().toString()
        createDataframe(confKey, confValue1).createOrReplaceTempView("m")
        spark.sparkContext.setLocalProperty(confKey, confValue1)
        assert(sql("SELECT * FROM l WHERE EXISTS (SELECT * FROM m)").collect().length == 1)

        // change the conf value and assert again
        val confValue2 = UUID.randomUUID().toString()
        createDataframe(confKey, confValue2).createOrReplaceTempView("n")
        spark.sparkContext.setLocalProperty(confKey, confValue2)
        assert(sql("SELECT * FROM l WHERE EXISTS (SELECT * FROM n)").collect().length == 1)
      }
    }
  }

  test("SPARK-22590 propagate local properties to broadcast execution thread") {
    withSQLConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD.key -> "1") {
      val df1 = Seq(true).toDF()
      val confKey = "spark.sql.y"
      val confValue1 = UUID.randomUUID().toString()
      val confValue2 = UUID.randomUUID().toString()

      def generateBroadcastDataFrame(confKey: String, confValue: String): Dataset[Boolean] = {
        val df = spark.range(1).mapPartitions { _ =>
          Iterator(TaskContext.get().getLocalProperty(confKey) == confValue)
        }
        df.hint("broadcast")
      }

      // set local property and assert
      val df2 = generateBroadcastDataFrame(confKey, confValue1)
      spark.sparkContext.setLocalProperty(confKey, confValue1)
      val checks = df1.join(df2).collect()
      assert(checks.forall(_.toSeq == Seq(true, true)))

      // change local property and re-assert
      val df3 = generateBroadcastDataFrame(confKey, confValue2)
      spark.sparkContext.setLocalProperty(confKey, confValue2)
      val checks2 = df1.join(df3).collect()
      assert(checks2.forall(_.toSeq == Seq(true, true)))
    }
  }
}

case class SQLConfAssertPlan(confToCheck: Seq[(String, String)]) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    sparkContext
      .parallelize(0 until 2, 2)
      .mapPartitions { it =>
        val confs = SQLConf.get
        confToCheck.foreach { case (key, expectedValue) =>
          assert(confs.getConfString(key) == expectedValue)
        }
        it.map(i => InternalRow.fromSeq(Seq(i)))
      }
  }

  override def output: Seq[Attribute] = Seq.empty
}

case class FakeQueryExecution(spark: SparkSession, physicalPlan: SparkPlan)
    extends QueryExecution(spark, LocalRelation()) {
  override lazy val sparkPlan: SparkPlan = physicalPlan
  override lazy val executedPlan: SparkPlan = physicalPlan
}
