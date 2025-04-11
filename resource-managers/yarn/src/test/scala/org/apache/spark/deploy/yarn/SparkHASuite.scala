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

  /**
   * This test is used to check the data integrity in case of Fetch Failures of ResultStage which
   * is directly dependent on one or more inDeterminate stage.
   * To reproduce the bugs related to race condition and straightforward failure of inDeterminate
   * shuffle stage, the test requires a very specific pattern of data distribution.
   * The situation envisaged for testing is as below:
   *              Left Outer Join
   *                 |
   *   KeyOuter:Int =         KeyInner: Int
   *     |                         |
   *  Table Outer                  Table Inner
   *  strLeft,
   *  KeyOuterUntransformed      strRight, KeyInnner
   *
   *  For simplicity  each table has rows such that they are partitioned using strLeft and strRight
   *  and each table will be creating only two partitions:
   *
   *  So for Table Outer the data distribution is
   *
   *  Partition 1      Partition2
   *  strLeft = aa     strLeft = bb
   *  aa, 1             bb, null
   *  aa, 0             bb, null
   *  aa, 1             bb, null
   *  aa, 0             bb, null
   *  aa, 1             bb, null
   *
   *
   * For Table  Inner there is only 1 partition
   * *  Partition 1
   * strRight = bb
   * bb, 1
   * bb, 0
   *
   * The joining key for table outer is keyOuter =  if KeyOuterUntransformed is not null ,
   * then return the value
   * else  return MyUDF
   *
   * ( Now since there are 6 rows which will be null, the UDF is written such that
   * after every 6 invocations, the value returned will be incremented by 1., starting with initial
   * value of 12.
   * so if the table is read for 1st time, it null, will get values 12, next time 13, and so on
   *
   *
   * The join executed is forced to use Shuffle Hash ( i.e hashing on joining key)
   * So on join execution,
   * Shuffle Stage  during mapping operation for TableOuter will have two partition tasks:
   * Partition : aa ,  the Block File will contain data for two reduced partitions ( 0, 1)
   * BlockFile1 : aa = (hash = 0)  and ( hash = 1)
   * BlockFile2 : bb =  ( since the bb rows have all null, the value taken by keyOuter will be
   * 12) i.e hash = 0  ( if the bb rows are read again, then value will be 13, hash = 1)
   * So its clear that BlockFile for partition bb, for map phase shuffle stage, will contain values
   * which reduce to exactly one partition at a time, and oscillate between 0 & 1 on every total
   * read.
   * Which means that if any node is randomly killed and if it contains that Block File of
   * partition bb for Table outer . It will be lost and its guaranteed to effect only 1 result
   * partition at a time ( 0 or 1)
   *
   * The mini cluster starts two executor VMs to run the test
   * and xecutes this join query multiple times, and a separate thread randomly kills one of the
   * process.
   *
   * Clearly because there going to be two reduced partitions ( 0, and 1), there will be 2 result
   * tasks.
   *
   * Following situations can arise as far as outcome of the query is concerned
   * 1) Both the result tasks ( corresponding to partition 0 and 1 ) reach driver successfully.
   * Implying query executed fine
   *
   * 2) The first result task failed and failing shuffle stage is inDeterminate.
   * Now since the first result task ( partition 0) failed,
   * code should ideally, retry both the partitions ( 0 & 1), and not just failed partition 0.
   * and in the window of retrying, even if successful partition  1 arrives, it should be discarded.
   *
   * The reason why retry of both 0 and 1 are needed , even if say, 1 is successful, is because
   * partition corresponding to 0 failed in fetch.  Now 0th  reduce partition will spawn two map
   * tasks for Table outer because of two partitions : ( aa, bb).
   * Lets say the 0 th reduce partition inistally contained all the rows corresponding to bb (
   * because their hash evaluated to 0).
   * Now on re-execution of result partition 0,
   * map task , will create a Block File aa ( for hash 0 and 1)
   * and map task for bb will create a Block File bb ( for hash  1) only ( whereas eariler these
   * rows of bb were part of reduced partition 0). But now due to inDeterminacy , it is part of
   * reduced result partition 1.
   * But if we do not retry both result tasks ( 0 & 1), and if  result task correspnding to
   * partition 1 is assumed successful, the rows corresponding to map stage partition
   * bb will be lost
   *
   * So for correct result both partitions need to be retried.
   *
   * 3) The first result task was successful and output committed, subsequent result task failed, in
   * which case the query needs to be aborted ( as a result commited cannot be reverted yet0
   *
   * 4) The first result task failed, but failed shuffle stage is determinate.. Even in this case
   * all the result tasks need to be retried, because at this point its not known whether
   * inDeterminate shuffle stage is also lost or not.and if say we accept a successful result task
   * and internally it is found that inDeterminate stage shuffle files are also lost, then the
   * re-execution of just the failing result task, will yield wrong results/
   *
   * 5) The first result task was successful, subseuqent result task failed and failing stage is
   * determinate, then the query should abort. Because for the same reason that we do not the state
   * of indeterminate shuffle stage.
   *
   *
   * If the query succeeds , the result should be as expected and logically correct ( the rows
   * corresponding to strLeft = bb, are bound to not match any inner rows and should always be
   * included in the result due to outer join), size of result set should be as expected.
   *
   *
   */
  ignore("bug SPARK-51016 and SPARK-51272: Indeterminate stage retry giving wrong results") {
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
