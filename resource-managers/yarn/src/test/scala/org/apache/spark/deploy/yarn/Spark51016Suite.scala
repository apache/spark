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
import com.google.common.io.Files
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.{SparkContext, SparkException, TaskContext}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{DRIVER_MEMORY, EXECUTOR_CORES, EXECUTOR_INSTANCES, EXECUTOR_MEMORY}
import org.apache.spark.tags.ExtendedYarnTest

@ExtendedYarnTest
class Spark51016Suite extends BaseYarnClusterSuite {

  override def newYarnConfig(): YarnConfiguration = new YarnConfiguration()


  test("run Spark in yarn-client mode with different configurations, ensuring redaction") {
    testBasicYarnApp(true,
      Map(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2",
        "spark.task.maxFailures" -> "4",
        "spark.network.timeout" -> "100000s",
        "spark.shuffle.sort.bypassMergeThreshold" -> "1",
        "spark.sql.files.maxPartitionNum" -> "2",
        "spark.sql.files.minPartitionNum" -> "2",
        DRIVER_MEMORY.key -> "512m",
        EXECUTOR_CORES.key -> "2",
        EXECUTOR_MEMORY.key -> "512m",
        EXECUTOR_INSTANCES.key -> "2",
        "spark.ui.port" -> "4040",
        "spark.ui.enabled" -> "true"
      ))
  }

  private def testBasicYarnApp(clientMode: Boolean, conf: Map[String, String] = Map()): Unit = {
    val result = File.createTempFile("result", null, tempDir)
    val finalState = runSpark(clientMode, mainClassName(Spark51016Suite.getClass),
      appArgs = Seq(result.getAbsolutePath()),
      extraConf = conf)
    checkResult(finalState, result)
  }
}

private object Spark51016Suite extends Logging {

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
    if (args.length != 1) {
      // scalastyle:off println
      System.err.println(
        s"""
           |Invalid command line: ${args.mkString(" ")}
           |
           |Usage: Spark51016Suite [result file]
        """.stripMargin)
      // scalastyle:on println
      System.exit(1)
    }

    val spark = SparkSession.builder().appName("Spark51016Suite").getOrCreate()
    val sc = SparkContext.getOrCreate()

    val status = new File(args(0))
    var result = "failure"
    try {
      createBaseTables(spark)
      val outerjoin: DataFrame = getOuterJoinDF(spark)

   //   println("Initial data")
      //  outerjoin.show(100)

      val correctRows = outerjoin.collect()
      TaskContext.unset()
      for (i <- 0 until 100) {
        try {
          println("before query exec")
        //  TaskContext.setFailResult()
          val rowsAfterRetry = getOuterJoinDF(spark).collect()
        //  TaskContext.unsetFailResult()

          if (correctRows.length != rowsAfterRetry.length) {
            println(s"encounterted test failure incorrect query result. run  index = $i ")
          }

          assert(correctRows.length == rowsAfterRetry.length)

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
          }
          )
          assert(retriedResults.isEmpty)
          println(s"found successful query exec on  iter index = $i")
        } catch {
          case se: SparkException if se.getMessage.contains("Please eliminate the" +
            " indeterminacy by checkpointing the RDD before repartition and try again") =>
            println(s"correctly encountered exception on iter index = $i")
          // OK expected
        }
      }
      Thread.sleep(1000000)
      result = "success"
    } finally {
      Files.asCharSink(status, StandardCharsets.UTF_8).write(result)
      sc.stop()
    }
  }

}