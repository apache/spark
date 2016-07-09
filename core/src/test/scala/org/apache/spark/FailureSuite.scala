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

package org.apache.spark

import java.io.{IOException, NotSerializableException, ObjectInputStream}

import org.apache.spark.memory.TestMemoryConsumer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NonSerializable

// Common state shared by FailureSuite-launched tasks. We use a global object
// for this because any local variables used in the task closures will rightfully
// be copied for each task, so there's no other way for them to share state.
object FailureSuiteState {
  var tasksRun = 0
  var tasksFailed = 0

  def clear() {
    synchronized {
      tasksRun = 0
      tasksFailed = 0
    }
  }
}

class FailureSuite extends SparkFunSuite with LocalSparkContext {

  // Run a 3-task map job in which task 1 deterministically fails once, and check
  // whether the job completes successfully and we ran 4 tasks in total.
  test("failure in a single-stage job") {
    sc = new SparkContext("local[1,2]", "test")
    val results = sc.makeRDD(1 to 3, 3).map { x =>
      FailureSuiteState.synchronized {
        FailureSuiteState.tasksRun += 1
        if (x == 1 && FailureSuiteState.tasksFailed == 0) {
          FailureSuiteState.tasksFailed += 1
          throw new Exception("Intentional task failure")
        }
      }
      x * x
    }.collect()
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    assert(results.toList === List(1, 4, 9))
    FailureSuiteState.clear()
  }

  // Run a map-reduce job in which a reduce task deterministically fails once.
  test("failure in a two-stage job") {
    sc = new SparkContext("local[1,2]", "test")
    val results = sc.makeRDD(1 to 3).map(x => (x, x)).groupByKey(3).map {
      case (k, v) =>
        FailureSuiteState.synchronized {
          FailureSuiteState.tasksRun += 1
          if (k == 1 && FailureSuiteState.tasksFailed == 0) {
            FailureSuiteState.tasksFailed += 1
            throw new Exception("Intentional task failure")
          }
        }
        (k, v.head * v.head)
      }.collect()
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    assert(results.toSet === Set((1, 1), (2, 4), (3, 9)))
    FailureSuiteState.clear()
  }

  // Run a map-reduce job in which the map stage always fails.
  test("failure in a map stage") {
    sc = new SparkContext("local", "test")
    val data = sc.makeRDD(1 to 3).map(x => { throw new Exception; (x, x) }).groupByKey(3)
    intercept[SparkException] {
      data.collect()
    }
    // Make sure that running new jobs with the same map stage also fails
    intercept[SparkException] {
      data.collect()
    }
  }

  test("failure because task results are not serializable") {
    sc = new SparkContext("local[1,1]", "test")
    val results = sc.makeRDD(1 to 3).map(x => new NonSerializable)

    val thrown = intercept[SparkException] {
      results.collect()
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("serializable") ||
      thrown.getCause.getClass === classOf[NotSerializableException],
      "Exception does not contain \"serializable\": " + thrown.getMessage)

    FailureSuiteState.clear()
  }

  test("failure because task closure is not serializable") {
    sc = new SparkContext("local[1,1]", "test")
    val a = new NonSerializable

    // Non-serializable closure in the final result stage
    val thrown = intercept[SparkException] {
      sc.parallelize(1 to 10, 2).map(x => a).count()
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getMessage.contains("NotSerializableException") ||
      thrown.getCause.getClass === classOf[NotSerializableException])

    // Non-serializable closure in an earlier stage
    val thrown1 = intercept[SparkException] {
      sc.parallelize(1 to 10, 2).map(x => (x, a)).partitionBy(new HashPartitioner(3)).count()
    }
    assert(thrown1.getClass === classOf[SparkException])
    assert(thrown1.getMessage.contains("NotSerializableException") ||
      thrown1.getCause.getClass === classOf[NotSerializableException])

    // Non-serializable closure in foreach function
    val thrown2 = intercept[SparkException] {
      // scalastyle:off println
      sc.parallelize(1 to 10, 2).foreach(x => println(a))
      // scalastyle:on println
    }
    assert(thrown2.getClass === classOf[SparkException])
    assert(thrown2.getMessage.contains("NotSerializableException") ||
      thrown2.getCause.getClass === classOf[NotSerializableException])

    FailureSuiteState.clear()
  }

  test("managed memory leak error should not mask other failures (SPARK-9266") {
    val conf = new SparkConf().set("spark.unsafe.exceptionOnMemoryLeak", "true")
    sc = new SparkContext("local[1,1]", "test", conf)

    // If a task leaks memory but fails due to some other cause, then make sure that the original
    // cause is preserved
    val thrownDueToTaskFailure = intercept[SparkException] {
      sc.parallelize(Seq(0)).mapPartitions { iter =>
        val c = new TestMemoryConsumer(TaskContext.get().taskMemoryManager())
        TaskContext.get().taskMemoryManager().allocatePage(128, c)
        throw new Exception("intentional task failure")
        iter
      }.count()
    }
    assert(thrownDueToTaskFailure.getMessage.contains("intentional task failure"))

    // If the task succeeded but memory was leaked, then the task should fail due to that leak
    val thrownDueToMemoryLeak = intercept[SparkException] {
      sc.parallelize(Seq(0)).mapPartitions { iter =>
        val c = new TestMemoryConsumer(TaskContext.get().taskMemoryManager())
        TaskContext.get().taskMemoryManager().allocatePage(128, c)
        iter
      }.count()
    }
    assert(thrownDueToMemoryLeak.getMessage.contains("memory leak"))
  }

  // Run a 3-task map job in which task 1 always fails with a exception message that
  // depends on the failure number, and check that we get the last failure.
  test("last failure cause is sent back to driver") {
    sc = new SparkContext("local[1,2]", "test")
    val data = sc.makeRDD(1 to 3, 3).map { x =>
      FailureSuiteState.synchronized {
        FailureSuiteState.tasksRun += 1
        if (x == 3) {
          FailureSuiteState.tasksFailed += 1
          throw new UserException("oops",
            new IllegalArgumentException("failed=" + FailureSuiteState.tasksFailed))
        }
      }
      x * x
    }
    val thrown = intercept[SparkException] {
      data.collect()
    }
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getCause.getClass === classOf[UserException])
    assert(thrown.getCause.getMessage === "oops")
    assert(thrown.getCause.getCause.getClass === classOf[IllegalArgumentException])
    assert(thrown.getCause.getCause.getMessage === "failed=2")
    FailureSuiteState.clear()
  }

  test("failure cause stacktrace is sent back to driver if exception is not serializable") {
    sc = new SparkContext("local", "test")
    val thrown = intercept[SparkException] {
      sc.makeRDD(1 to 3).foreach { _ => throw new NonSerializableUserException }
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getCause === null)
    assert(thrown.getMessage.contains("NonSerializableUserException"))
    FailureSuiteState.clear()
  }

  test("failure cause stacktrace is sent back to driver if exception is not deserializable") {
    sc = new SparkContext("local", "test")
    val thrown = intercept[SparkException] {
      sc.makeRDD(1 to 3).foreach { _ => throw new NonDeserializableUserException }
    }
    assert(thrown.getClass === classOf[SparkException])
    assert(thrown.getCause === null)
    assert(thrown.getMessage.contains("NonDeserializableUserException"))
    FailureSuiteState.clear()
  }

  // Run a 3-task map stage where one task fails once.
  test("failure in tasks in a submitMapStage") {
    sc = new SparkContext("local[1,2]", "test")
    val rdd = sc.makeRDD(1 to 3, 3).map { x =>
      FailureSuiteState.synchronized {
        FailureSuiteState.tasksRun += 1
        if (x == 1 && FailureSuiteState.tasksFailed == 0) {
          FailureSuiteState.tasksFailed += 1
          throw new Exception("Intentional task failure")
        }
      }
      (x, x)
    }
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
    sc.submitMapStage(dep).get()
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    FailureSuiteState.clear()
  }

  test("failure because cached RDD partitions are missing from DiskStore (SPARK-15736)") {
    sc = new SparkContext("local[1,2]", "test")
    val rdd = sc.parallelize(1 to 2, 2).persist(StorageLevel.DISK_ONLY)
    rdd.count()
    // Directly delete all files from the disk store, triggering failures when reading cached data:
    SparkEnv.get.blockManager.diskBlockManager.getAllFiles().foreach(_.delete())
    // Each task should fail once due to missing cached data, but then should succeed on its second
    // attempt because the missing cache locations will be purged and the blocks will be recomputed.
    rdd.count()
  }

  test("SPARK-16304: Link error should not crash executor") {
    sc = new SparkContext("local[1,2]", "test")
    intercept[SparkException] {
      sc.parallelize(1 to 2).foreach { i =>
        throw new LinkageError()
      }
    }
  }

  // TODO: Need to add tests with shuffle fetch failures.
}

class UserException(message: String, cause: Throwable)
  extends RuntimeException(message, cause)

class NonSerializableUserException extends RuntimeException {
  val nonSerializableInstanceVariable = new NonSerializable
}

class NonDeserializableUserException extends RuntimeException {
  private def readObject(in: ObjectInputStream): Unit = {
    throw new IOException("Intentional exception during deserialization.")
  }
}
