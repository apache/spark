package spark.scheduler

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import spark.TaskContext
import spark.RDD
import spark.SparkContext
import spark.Split

class TaskContextSuite extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = _

  after {
    if (sc != null) {
      sc.stop()
      sc = null
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port")
  }

  test("Calls executeOnCompleteCallbacks after failure") {
    var completed = false
    sc = new SparkContext("local", "test")
    val rdd = new RDD[String](sc) {
      override val splits = Array[Split](StubSplit(0))
      override val dependencies = List()
      override def compute(split: Split, context: TaskContext) = {
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

  case class StubSplit(val index: Int) extends Split
}