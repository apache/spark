package spark

import org.scalatest.FunSuite
import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import scala.util.Random

class UtilsSuite extends FunSuite {

  test("memoryBytesToString") {
    assert(Utils.memoryBytesToString(10) === "10.0 B")
    assert(Utils.memoryBytesToString(1500) === "1500.0 B")
    assert(Utils.memoryBytesToString(2000000) === "1953.1 KB")
    assert(Utils.memoryBytesToString(2097152) === "2.0 MB")
    assert(Utils.memoryBytesToString(2306867) === "2.2 MB")
    assert(Utils.memoryBytesToString(5368709120L) === "5.0 GB")
    assert(Utils.memoryBytesToString(5L * 1024L * 1024L * 1024L * 1024L) === "5.0 TB")
  }

  test("copyStream") {
    //input array initialization
    val bytes = Array.ofDim[Byte](9000)
    Random.nextBytes(bytes)

    val os = new ByteArrayOutputStream()
    Utils.copyStream(new ByteArrayInputStream(bytes), os)

    assert(os.toByteArray.toList.equals(bytes.toList))
  }

  test("memoryStringToMb"){
    assert(Utils.memoryStringToMb("1") == 0)
    assert(Utils.memoryStringToMb("1048575") == 0)
    assert(Utils.memoryStringToMb("3145728") == 3)

    assert(Utils.memoryStringToMb("1024k") == 1)
    assert(Utils.memoryStringToMb("5000k") == 4)
    assert(Utils.memoryStringToMb("4024k") == Utils.memoryStringToMb("4024K"))

    assert(Utils.memoryStringToMb("1024m") == 1024)
    assert(Utils.memoryStringToMb("5000m") == 5000)
    assert(Utils.memoryStringToMb("4024m") == Utils.memoryStringToMb("4024M"))

    assert(Utils.memoryStringToMb("2g") == 2048)
    assert(Utils.memoryStringToMb("3g") == Utils.memoryStringToMb("3G"))

    assert(Utils.memoryStringToMb("2t") == 2097152)
    assert(Utils.memoryStringToMb("3t") == Utils.memoryStringToMb("3T"))
  }
}

