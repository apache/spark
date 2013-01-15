package spark

import org.scalatest.FunSuite
import org.scalatest.prop.Checkers

import scala.collection.mutable.ArrayBuffer

import SparkContext._

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

class FailureSuite extends FunSuite with LocalSparkContext {
  
  // Run a 3-task map job in which task 1 deterministically fails once, and check
  // whether the job completes successfully and we ran 4 tasks in total.
  test("failure in a single-stage job") {
    sc = new SparkContext("local[1,1]", "test")
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
    assert(results.toList === List(1,4,9))
    FailureSuiteState.clear()
  }

  // Run a map-reduce job in which a reduce task deterministically fails once.
  test("failure in a two-stage job") {
    sc = new SparkContext("local[1,1]", "test")
    val results = sc.makeRDD(1 to 3).map(x => (x, x)).groupByKey(3).map {
      case (k, v) => 
        FailureSuiteState.synchronized {
          FailureSuiteState.tasksRun += 1
          if (k == 1 && FailureSuiteState.tasksFailed == 0) {
            FailureSuiteState.tasksFailed += 1
            throw new Exception("Intentional task failure")
          }
        }
        (k, v(0) * v(0))
      }.collect()
    FailureSuiteState.synchronized {
      assert(FailureSuiteState.tasksRun === 4)
    }
    assert(results.toSet === Set((1, 1), (2, 4), (3, 9)))
    FailureSuiteState.clear()
  }

  test("failure because task results are not serializable") {
    sc = new SparkContext("local[1,1]", "test")
    val results = sc.makeRDD(1 to 3).map(x => new NonSerializable)

    val thrown = intercept[spark.SparkException] {
      results.collect()
    }
    assert(thrown.getClass === classOf[spark.SparkException])
    assert(thrown.getMessage.contains("NotSerializableException"))

    FailureSuiteState.clear()
  }

  // TODO: Need to add tests with shuffle fetch failures.
}


