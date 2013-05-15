package spark.deploy.client

import spark.util.AkkaUtils
import spark.{Logging, Utils}
import spark.deploy.{Command, ApplicationDescription}

private[spark] object TestClient {

  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got app ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }

    def executorAdded(id: String, workerId: String, hostPort: String, cores: Int, memory: Int) {}

    def executorRemoved(id: String, message: String, exitStatus: Option[Int]) {}
  }

  def main(args: Array[String]) {
    val url = args(0)
    val (actorSystem, port) = AkkaUtils.createActorSystem("spark", Utils.localIpAddress, 0)
    val desc = new ApplicationDescription(
      "TestClient", 1, 512, Command("spark.deploy.client.TestExecutor", Seq(), Map()), "dummy-spark-home", "ignored")
    val listener = new TestListener
    val client = new Client(actorSystem, url, desc, listener)
    client.start()
    actorSystem.awaitTermination()
  }
}
