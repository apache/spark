package spark.ui

import org.scalatest.FunSuite
import org.eclipse.jetty.server.Server

class UISuite extends FunSuite {
  test("jetty port increases under contention") {
    val startPort = 33333
    val server = new Server(startPort)
    server.start()
    val (jettyServer, boundPort) = JettyUtils.startJettyServer("localhost", startPort, Seq())
    assert(boundPort === startPort + 1)
  }

}
