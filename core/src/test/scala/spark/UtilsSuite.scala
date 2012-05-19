package spark

import org.scalatest.FunSuite
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import util.Random

class UtilsSuite extends FunSuite {

  test("memoryBytesToString") {
    assert(Utils.memoryBytesToString(10) === "10.0B")
    assert(Utils.memoryBytesToString(1500) === "1500.0B")
    assert(Utils.memoryBytesToString(2000000) === "1953.1KB")
    assert(Utils.memoryBytesToString(2097152) === "2.0MB")
    assert(Utils.memoryBytesToString(2306867) === "2.2MB")
    assert(Utils.memoryBytesToString(5368709120L) === "5.0GB")
  }

  test("copyStream") {
    //input array initialization
    val bytes = Array.ofDim[Byte](9000)
    Random.nextBytes(bytes)

    val os = new ByteArrayOutputStream()
    Utils.copyStream(new ByteArrayInputStream(bytes), os)

    assert(os.toByteArray.toList.equals(bytes.toList))
  }
}

