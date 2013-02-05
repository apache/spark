package spark.scheduler

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import spark.TaskContext
import spark.RDD
import spark.SparkContext
import spark.Split
import spark.LocalSparkContext

class TaskContextSuite extends FunSuite with BeforeAndAfter with LocalSparkContext {

  test("Calls executeOnCompleteCallbacks after failure") {
    var completed = false
    sc = new SparkContext("local", "test")
    val rdd = new RDD[String](sc, List()) {
      override def getSplits = Array[Split](StubSplit(0))
      override def compute(split: Split, context: TaskContext) = {
        context.addOnCompleteCallback(_ => completed = true)
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

  case class StubSplit(val index: Int) extends Split
}