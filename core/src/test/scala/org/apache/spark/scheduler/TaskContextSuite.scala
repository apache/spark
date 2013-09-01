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
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.LocalSparkContext

class TaskContextSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  test("Calls executeOnCompleteCallbacks after failure") {
    var completed = false
    sc = new SparkContext("local", "test")
    val rdd = new RDD[String](sc, List()) {
      override def getPartitions = Array[Partition](StubPartition(0))
      override def compute(split: Partition, context: TaskContext) = {
        context.addOnCompleteCallback(() => completed = true)
        sys.error("failed")
      }
    }
    val func = (c: TaskContext, i: Iterator[String]) => i.next
    val task = new ResultTask[String, String](0, rdd, func, 0, Seq(), 0)
    intercept[RuntimeException] {
      task.run(0)
    }
    assert(completed === true)
  }

  case class StubPartition(val index: Int) extends Partition
}
