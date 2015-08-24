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
package org.apache.spark.scheduler

import org.apache.spark.{SparkContext, LocalSparkContext, SparkFunSuite}

class SkippedStageSuite extends SparkFunSuite with LocalSparkContext {

  class PrintStageSubmission extends SparkListener {
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      println("submitting stage " + stageSubmitted.stageInfo.stageId)
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      println("completed stage " + stageCompleted.stageInfo.stageId)
    }
    
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
      println("submitting job w/ stages: " + jobStart.stageIds)
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      println("completed job")
    }


  }
  
  test("skipped stages") {
    sc = new SparkContext("local", "test")
    sc.addSparkListener(new PrintStageSubmission)
    val partitioner = new org.apache.spark.HashPartitioner(10)
    val d3 = sc.parallelize(1 to 100).map { x => (x % 10) -> x}.partitionBy(partitioner)
    (0 until 5).foreach { idx =>
      val otherData = sc.parallelize(1 to (idx * 100)).map{ x => (x % 10) -> x}.partitionBy(partitioner)
      val joined = otherData.join(d3)
      println("debug string: " + joined.toDebugString)
      println(idx + " ---> " + joined.count())
    }
  }


  test("job shares long lineage w/ caching") {
    sc = new SparkContext("local", "test")
    sc.addSparkListener(new PrintStageSubmission)
    val partitioner = new org.apache.spark.HashPartitioner(10)

    val d1 = sc.parallelize(1 to 100).map { x => (x % 10) -> x}.partitionBy(partitioner)
    val d2 = d1.mapPartitions{itr => itr.map{ case(x,y) => x -> (y + 1)}}.partitionBy(partitioner)
    val d3 = d2.mapPartitions{itr => itr.map{ case(x,y) => x -> (y + 1)}}.partitionBy(partitioner)
    d3.cache()
    d3.count()
    d3.count()
  }

}
