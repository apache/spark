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


package org.apache.spark.deploy.worker

import org.apache.spark.{SparkConf, SparkFunSuite}


class WorkerArgumentsTest extends SparkFunSuite {

  test("Memory can't be set to 0 when cmd line args leave off M or G") {
    val conf = new SparkConf
    val args = Array("-m", "10000", "spark://localhost:0000  ")
    intercept[IllegalStateException] {
      new WorkerArguments(args, conf)
    }
  }


  test("Memory can't be set to 0 when SPARK_WORKER_MEMORY env property leaves off M or G") {
    val args = Array("spark://localhost:0000  ")

    class MySparkConf extends SparkConf(false) {
      override def getenv(name: String): String = {
        if (name == "SPARK_WORKER_MEMORY") "50000"
        else super.getenv(name)
      }

      override def clone: SparkConf = {
        new MySparkConf().setAll(getAll)
      }
    }
    val conf = new MySparkConf()
    intercept[IllegalStateException] {
      new WorkerArguments(args, conf)
    }
  }

  test("Memory correctly set when SPARK_WORKER_MEMORY env property appends G") {
    val args = Array("spark://localhost:0000  ")

    class MySparkConf extends SparkConf(false) {
      override def getenv(name: String): String = {
        if (name == "SPARK_WORKER_MEMORY") "5G"
        else super.getenv(name)
      }

      override def clone: SparkConf = {
        new MySparkConf().setAll(getAll)
      }
    }
    val conf = new MySparkConf()
    val workerArgs = new WorkerArguments(args, conf)
    assert(workerArgs.memory === 5120)
  }

  test("Memory correctly set from args with M appended to memory value") {
    val conf = new SparkConf
    val args = Array("-m", "10000M", "spark://localhost:0000  ")

    val workerArgs = new WorkerArguments(args, conf)
    assert(workerArgs.memory === 10000)

  }

}
