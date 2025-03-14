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

package org.apache.spark.deploy.yarn

import java.io.File
import java.nio.charset.StandardCharsets

import scala.concurrent.duration._

import com.google.common.io.Files
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, EXECUTOR_MEMORY}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.tags.ExtendedYarnTest


@ExtendedYarnTest
class SparkHASuite extends BaseYarnClusterSuite {
  override def newYarnConfig(): YarnConfiguration = new YarnConfiguration()
  test("bug SPARK-51016 and SPARK-51272: Indeterminate stage retry giving wrong results") {
    testBasicYarnApp(
      Map(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        "spark.task.maxFailures" -> "8",
        "spark.network.timeout" -> "100000s",
        "spark.shuffle.sort.bypassMergeThreshold" -> "1",
        "spark.sql.files.maxPartitionNum" -> "2",
        "spark.sql.files.minPartitionNum" -> "2",
        DRIVER_MEMORY.key -> "512m",
        EXECUTOR_CORES.key -> "1",
        EXECUTOR_MEMORY.key -> "512m",
        EXECUTOR_INSTANCES.key -> "2",
        "spark.ui.enabled" -> "false",
        "spark.yarn.max.executor.failures" -> "100000"
      ))
  }

  private def testBasicYarnApp(conf: Map[String, String] = Map()): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(
      clientMode = true,
      mainClassName(SparkHASuite.getClass),
      appArgs = Seq(result.getAbsolutePath),
      extraConf = conf,
      testTimeOutParams = TimeoutParams(30.minutes, 30.seconds))
    checkResult(finalState, result)
  }
}

private object SparkHASuite extends Logging {

  object Counter {
    var counter = 0
    var retVal = 12

    def getHash(): Int = this.synchronized {
      counter += 1
      val x = retVal
      if (counter % 6 == 0) {
        retVal += 1
      }
      x
    }
  }

  private def getOuterJoinDF(spark: SparkSession) = {
    import org.apache.spark.sql.functions.udf
    val myudf = udf(() => Counter.getHash()).asNondeterministic()
    spark.udf.register("myudf", myudf.asNondeterministic())

    val leftOuter = spark.table("outer").select(
      col("strleft"), when(isnull(col("pkLeftt")), myudf().
        cast(IntegerType)).
        otherwise(col("pkLeftt")).as("pkLeft"))

    val innerRight = spark.table("inner")

    val outerjoin = leftOuter.hint("SHUFFLE_HASH").
      join(innerRight, col("pkLeft") === col("pkRight"), "left_outer")
    outerjoin
  }

  def createBaseTables(spark: SparkSession): Unit = {
    spark.sql("drop table if exists outer ")
    spark.sql("drop table if exists inner ")
    val data = Seq(
      (java.lang.Integer.valueOf(0), "aa"),
      (java.lang.Integer.valueOf(1), "aa"),
      (java.lang.Integer.valueOf(1), "aa"),
      (java.lang.Integer.valueOf(0), "aa"),
      (java.lang.Integer.valueOf(0), "aa"),
      (java.lang.Integer.valueOf(0), "aa"),
      (null, "bb"),
      (null, "bb"),
      (null, "bb"),
      (null, "bb"),
      (null, "bb"),
      (null, "bb")
    )
    val data1 = Seq(
      (java.lang.Integer.valueOf(0), "bb"),
      (java.lang.Integer.valueOf(1), "bb"))
    val outerDf = spark.createDataset(data)(
      Encoders.tuple(Encoders.INT, Encoders.STRING)).toDF("pkLeftt", "strleft")
    this.logInfo("saving outer table")
    outerDf.write.format("parquet").partitionBy("strleft").saveAsTable("outer")
    val innerDf = spark.createDataset(data1)(
      Encoders.tuple(Encoders.INT, Encoders.STRING)).toDF("pkRight", "strright")
    this.logInfo("saving inner table")
    innerDf.write.format("parquet").partitionBy("strright").saveAsTable("inner")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark51016Suite")
      .config("spark.extraListeners", classOf[JobListener].getName)
      .getOrCreate()
    val sc = SparkContext.getOrCreate()

    val status = new File(args(0))
    var result = "failure"
    try {
      createBaseTables(spark)
      val outerjoin: DataFrame = getOuterJoinDF(spark)
      val correctRows = outerjoin.collect()
      JobListener.inKillMode = true
      JobListener.killWhen = KillPosition.KILL_IN_STAGE_SUBMISSION
      for (i <- 0 until 100) {
        if (i > 49) {
          JobListener.killWhen = KillPosition.KILL_IN_STAGE_COMPLETION
        }
        try {
          eventually(timeout(3.minutes), interval(100.milliseconds)) {
            assert(sc.getExecutorIds().size == 2)
          }
          val rowsAfterRetry = getOuterJoinDF(spark).collect()
          if (correctRows.length != rowsAfterRetry.length) {
            logInfo(s"encounterted test failure incorrect query result. run  index = $i ")
          }
          assert(correctRows.length == rowsAfterRetry.length,
            s"correct rows length = ${correctRows.length}," +
            s" retry rows length = ${rowsAfterRetry.length}")
          val retriedResults = rowsAfterRetry.toBuffer
          correctRows.foreach(r => {
            val index = retriedResults.indexWhere(x =>

              r.getString(0) == x.getString(0) &&
                (
                  (r.getInt(1) < 2 && r.getInt(1) == x.getInt(1) && r.getInt(2) == x.getInt(2) &&
                    r.getString(3) == x.getString(3))
                    ||
                    (r.isNullAt(2) && r.isNullAt(3) && x.isNullAt(3)
                      && x.isNullAt(2))

                  ))
            assert(index >= 0)
            retriedResults.remove(index)
          })
          assert(retriedResults.isEmpty)
          logInfo(s"found successful query exec on  iter index = $i")
        } catch {
          case se: SparkException if se.getMessage.contains("Please eliminate the" +
            " indeterminacy by checkpointing the RDD before repartition and try again") =>
            logInfo(s"correctly encountered exception on iter index = $i")
          // OK expected
        }
      }
      result = "success"
    } finally {
      Files.asCharSink(status, StandardCharsets.UTF_8).write(result)
      sc.stop()
    }
  }
}

object PIDGetter extends Logging {
  def getExecutorPIds: Seq[Int] = {
    import scala.sys.process._
    val output = Seq("ps", "-ef").#|(Seq("grep", "java")).#|(Seq("grep", "executor-id ")).lazyLines
    logInfo(s"pids obtained = ${output.mkString("\n")} ")
    if (output.nonEmpty && output.size == 4) {
      val execPidsStr = output.map(_.trim).filter(_.endsWith("--resourceProfileId 0"))
      logInfo(s"filtered Pid String obtained = ${execPidsStr.mkString("\n")} ")
      val pids = execPidsStr.map(str => str.split(" ")(1).toInt).sorted
      Seq(pids.head, pids(1))
    } else {
      Seq.empty
    }
  }

  def killExecutor(pid: Int): Unit = {
    import scala.sys.process._
    Seq("kill", "-9", pid.toString).!

  }

  def main(args: Array[String]): Unit = {
    getExecutorPIds
  }
}

private[spark] class JobListener extends SparkListener with Logging {
  private var count: Int = 0
  @volatile
  private var pidToKill: Option[Int] = None

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    if (JobListener.inKillMode) {
      val execids = PIDGetter.getExecutorPIds
      assert(execids.size == 2)
      pidToKill = Option(execids(count % 2))
      logInfo("Pid to kill = " + pidToKill)
      count += 1
    }
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (stageSubmitted.stageInfo.shuffleDepId.isEmpty && pidToKill.nonEmpty &&
      JobListener.killWhen == KillPosition.KILL_IN_STAGE_SUBMISSION) {
      val pid = pidToKill.get
      pidToKill = None
      logInfo(s"killing executor for pid = $pid")
      PIDGetter.killExecutor(pid)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    if (stageCompleted.stageInfo.shuffleDepId.exists(_ % 2 == count % 2) && pidToKill.nonEmpty &&
      JobListener.killWhen == KillPosition.KILL_IN_STAGE_COMPLETION) {
      val pid = pidToKill.get
      pidToKill = None
      logInfo(s"killing executor for pid = $pid")
      PIDGetter.killExecutor(pid)
    }
  }
}

object KillPosition extends Enumeration {
  type KillPosition = Value
  val KILL_IN_STAGE_SUBMISSION, KILL_IN_STAGE_COMPLETION, NONE = Value
}

object JobListener {
  @volatile
  var inKillMode: Boolean = false

  import KillPosition._
  @volatile
  var killWhen: KillPosition = NONE
}
