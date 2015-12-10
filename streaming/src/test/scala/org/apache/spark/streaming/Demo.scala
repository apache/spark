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

package org.apache.spark.streaming

import java.util.concurrent.TimeUnit
import org.apache.spark.HashPartitioner
import org.apache.spark.LocalSparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.util.ThreadUtils
import java.io.FileInputStream
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MapPartitionsRDD
import org.apache.spark.streaming.dstream.StateDStream
import org.apache.spark.serializer.Serializer
import org.apache.spark.rdd.ReliableCheckpointRDD
import org.apache.spark.streaming.dstream.CheckpointDumpFormat

/**
 * This is to demonstrate usage. Do NOT merge it to main branch!!!
 *
 * In this demo, we save latest status of StateDStream when process shutdown.
 * During restarting, we load it back and recover status.
 */
object Demo extends App {
  // define update method
  val updateFunc = (values: Seq[Int], state: Option[Int]) => {
    val currentCount = values.sum
    val previousCount = state.getOrElse(0)
    Some(currentCount + previousCount)
  }

  val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.flatMap(t => updateFunc(t._2, t._3).map { s => (t._1, s) })
  }

  val checkpointDir = "/home/maowei/checkpoint"

  // prepare StreamingContext
  val conf = new SparkConf()
    .setAppName("test")
    .setMaster("local")
    .set("spark.ui.enabled", "false")
  val ssc = new StreamingContext(conf, Seconds(1))
  ssc.checkpoint(checkpointDir)

  // new a DumpFormat instance
  val df = new CheckpointDumpFormat[String, Int]("WordCount")

  // read dumpped RDD back
  val initialRDD = df.load(ssc.sc , checkpointDir + "/WordCount")
  // val initialRDD = ssc.sc.parallelize(Seq(("hello", 1), ("world", 1)), 1)

  // define transformation and action
  val lines = ssc.textFileStream("/home/maowei/tmp")
  val wordDstream = lines.flatMap(_.split(" ")).map { x => (x, 1) }
  val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
    new HashPartitioner(ssc.sparkContext.defaultParallelism), true, initialRDD)

  // set DumpFormat for StateDStream instance
  stateDstream.setDumpFormat(df)

  stateDstream.print()

  // create another thread to stop ssc
  val thread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("stop-streamiing-thread")
  val scheduleTask = new Runnable() {
    override def run(): Unit = {
      ssc.stop(true, true)
    }
  }
  thread.schedule(scheduleTask, 5, TimeUnit.SECONDS)

  // start streaming context
  ssc.start()
  ssc.awaitTermination()
}
