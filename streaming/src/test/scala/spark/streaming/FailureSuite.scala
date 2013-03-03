package spark.streaming

import spark.Logging
import spark.streaming.util.MasterFailureTest
import StreamingContext._

import org.scalatest.{FunSuite, BeforeAndAfter}
import com.google.common.io.Files
import java.io.File
import org.apache.commons.io.FileUtils
import collection.mutable.ArrayBuffer


/**
 * This testsuite tests master failures at random times while the stream is running using
 * the real clock.
 */
class FailureSuite extends FunSuite with BeforeAndAfter with Logging {

  var directory = "FailureSuite"
  val numBatches = 30
  val batchDuration = Milliseconds(1000)

  before {
    FileUtils.deleteDirectory(new File(directory))
  }

  after {
    FileUtils.deleteDirectory(new File(directory))
  }

  test("multiple failures with map") {
    MasterFailureTest.testMap(directory, numBatches, batchDuration)
  }

  test("multiple failures with updateStateByKey") {
    MasterFailureTest.testUpdateStateByKey(directory, numBatches, batchDuration)
  }
}

