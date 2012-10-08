package spark.deploy.client

private[spark] object TestExecutor {
  def main(args: Array[String]) {
    println("Hello world!")
    while (true) {
      Thread.sleep(1000)
    }
  }
}
