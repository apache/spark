import spark._

object CpuHog {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println("Usage: CpuHog <master> <tasks> <threads_per_task>");
      System.exit(1)
    }
    val sc = new SparkContext(args(0), "CPU hog")
    val tasks = args(1).toInt
    val threads = args(2).toInt
    def task {
      for (i <- 0 until threads-1) {
        new Thread() {
          override def run {
            while(true) {}
          }
        }.start()
      }
      while(true) {}
    }
    sc.runTasks(Array.make(tasks, () => task))
  }
}
