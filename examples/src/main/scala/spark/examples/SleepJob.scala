package spark.examples

import spark._

object SleepJob {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: SleepJob <master> <tasks> <task_duration>");
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "Sleep job")
    val tasks = args(1).toInt
    val duration = args(2).toInt
    def task {
      val start = System.currentTimeMillis
      while (System.currentTimeMillis - start < duration * 1000L)
        Thread.sleep(200)
    }
    sc.runTasks(Array.make(tasks, () => task))
  }
}
