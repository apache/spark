package spark.ui

import org.scalatest.FunSuite
import org.eclipse.jetty.server.Server
import java.net.ServerSocket
import scala.util.{Failure, Success, Try}
import spark.Utils
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import java.io.{FileOutputStream, File}
import com.google.common.base.Charsets

class UISuite extends FunSuite {
  test("jetty port increases under contention") {
    val startPort = 33333
    val server = new Server(startPort)
    server.start()
    val (jettyServer1, boundPort1) = JettyUtils.startJettyServer("localhost", startPort, Seq())
    val (jettyServer2, boundPort2) = JettyUtils.startJettyServer("localhost", startPort, Seq())

    assert(boundPort1 === startPort + 1)
    assert(boundPort2 === startPort + 2)
  }

  test("jetty binds to port 0 correctly") {
    val (jettyServer, boundPort) = JettyUtils.startJettyServer("localhost", 0, Seq())
    assert(jettyServer.getState === "STARTED")
    assert(boundPort != 0)
    Try {new ServerSocket(boundPort)} match {
      case Success(s) => fail("Port %s doesn't seem used by jetty server".format(boundPort))
      case Failure  (e) =>
    }
  }
}
