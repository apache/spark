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

import java.util.concurrent.{ConcurrentHashMap, Executors}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually._

import org.apache.spark._
import org.apache.spark.rdd.RDD

class DAGSchedulerIntegrationSuite extends SparkFunSuite with LocalSparkContext {
  implicit val submissionPool =
    ExecutionContext.fromExecutor(Executors.newFixedThreadPool(3))

  test("blocking of DAGEventQueue due to a heavy pause job") {
    sc = new SparkContext("local", "DAGSchedulerIntegrationSuite")

    // form 3 rdds (2 quick and 1 with heavy dependency calculation)
    val simpleRDD1 = new DelegateRDD(sc, new PauseRDD(sc, 100))
    val heavyRDD = new DelegateRDD(sc, new PauseRDD(sc, 1000000))
    val simpleRDD2 = new DelegateRDD(sc, new PauseRDD(sc, 100))

    // submit all concurrently
    val finishedRDDs = new ConcurrentHashMap[DelegateRDD, String]()
    List(simpleRDD1, heavyRDD, simpleRDD2).foreach(rdd =>
      Future {
        rdd.collect
        finishedRDDs.put(rdd, rdd.toString)
    })

    // wait for certain time and see if quick jobs can finish
    eventually(timeout(10.seconds)) {
      assert(finishedRDDs.size() == 2)
      assert(
        finishedRDDs.contains(simpleRDD1.toString) &&
          finishedRDDs.contains(simpleRDD2.toString))
    }
  }
}

class DelegateRDD(sc: SparkContext, var dependency: PauseRDD)
    extends RDD[(Int, Int)](sc, List(new OneToOneDependency(dependency)))
    with Serializable {
  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] = {
    Nil.toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    Seq(new Partition {
      override def index: Int = 0
    }).toArray
  }

  override def toString: String = "DelegateRDD " + id
}

class PauseRDD(sc: SparkContext, var pauseDuartion: Long)
    extends RDD[(Int, Int)](sc, Seq.empty)
    with Serializable {
  override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] = {
    Nil.toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    Thread.sleep(pauseDuartion)
    Seq(new Partition {
      override def index: Int = 0
    }).toArray
  }

  override def toString: String = "PauseRDD " + id
}
