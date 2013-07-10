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

  test("string formatting of time durations") {
    val second = 1000
    val minute = second * 60
    val hour = minute * 60
    def str = Utils.msDurationToString(_)

    assert(str(123) === "123 ms")
    assert(str(second) === "1.0 s")
    assert(str(second + 462) === "1.5 s")
    assert(str(hour) === "1.00 h")
    assert(str(minute) === "1.0 m")
    assert(str(minute + 4 * second + 34) === "1.1 m")
    assert(str(10 * hour + minute + 4 * second) === "10.02 h")
    assert(str(10 * hour + 59 * minute + 59 * second + 999) === "11.00 h")
  }

  test("reading last n bytes of a file") {
    val tmpDir = Files.createTempDir()

    // File smaller than limit
    val f1Path = tmpDir + "/f1"
    val f1 = new FileOutputStream(f1Path)
    f1.write("a\nb\nc\nd".getBytes(Charsets.UTF_8))
    f1.close()
    assert(Utils.lastNBytes(f1Path, 1024) === "a\nb\nc\nd")

    // File larger than limit
    val f2Path = tmpDir + "/f2"
    val f2 = new FileOutputStream(f2Path)
    f2.write("1\n2\n3\n4\n5\n6\n7\n8\n".getBytes(Charsets.UTF_8))
    f2.close()
    assert(Utils.lastNBytes(f2Path, 8) === "5\n6\n7\n8\n")

    // Request limit too
    val f3Path = tmpDir + "/f2"
    val f3 = new FileOutputStream(f3Path)
    f3.write("1\n2\n3\n4\n5\n6\n7\n8\n".getBytes(Charsets.UTF_8))
    f3.close()
    assert(Utils.lastNBytes(f3Path, 8) === "5\n6\n7\n8\n")

    FileUtils.deleteDirectory(tmpDir)
  }

  test("reading offset bytes of a file") {
    val tmpDir2 = Files.createTempDir()
    val f1Path = tmpDir2 + "/f1"
    val f1 = new FileOutputStream(f1Path)
    f1.write("1\n2\n3\n4\n5\n6\n7\n8\n9\n".getBytes(Charsets.UTF_8))
    f1.close()

    // Read first few bytes
    assert(Utils.offsetBytes(f1Path, 0, 5) === "1\n2\n3")

    // Read some middle bytes
    assert(Utils.offsetBytes(f1Path, 4, 11) === "3\n4\n5\n6")

    // Read last few bytes
    assert(Utils.offsetBytes(f1Path, 12, 18) === "7\n8\n9\n")

    // Read some nonexistent bytes in the beginning
    assert(Utils.offsetBytes(f1Path, -5, 5) === "1\n2\n3")

    // Read some nonexistent bytes at the end
    assert(Utils.offsetBytes(f1Path, 12, 22) === "7\n8\n9\n")

    // Read some nonexistent bytes on both ends
    assert(Utils.offsetBytes(f1Path, -3, 25) === "1\n2\n3\n4\n5\n6\n7\n8\n9\n")

    FileUtils.deleteDirectory(tmpDir2)
  }
}
