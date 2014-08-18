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

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

class TaskContextSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  test("Calls executeOnCompleteCallbacks after failure") {
    TaskContextSuite.completed = false
    sc = new SparkContext("local", "test")
    val rdd = new RDD[String](sc, List()) {
      override def getPartitions = Array[Partition](StubPartition(0))
      override def compute(split: Partition, context: TaskContext) = {
        context.addTaskCompletionListener(context => TaskContextSuite.completed = true)
        sys.error("failed")
      }
    }
    val closureSerializer = SparkEnv.get.closureSerializer.newInstance()
    val func = (c: TaskContext, i: Iterator[String]) => i.next()
    val task = new ResultTask[String, String](
      0, sc.broadcast(closureSerializer.serialize((rdd, func)).array), rdd.partitions(0), Seq(), 0)
    intercept[RuntimeException] {
      task.run(0)
    }
    assert(TaskContextSuite.completed === true)
  }
}

private object TaskContextSuite {
  @volatile var completed = false
}

private case class StubPartition(index: Int) extends Partition
