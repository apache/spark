package spark

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.PrivateMethodTester
import org.scalatest.matchers.ShouldMatchers

class DummyClass1 {}

class DummyClass2 {
  val x: Int = 0
}

class DummyClass3 {
  val x: Int = 0
  val y: Double = 0.0
}

class DummyClass4(val d: DummyClass3) {
  val x: Int = 0
}

class SizeEstimatorSuite
  extends FunSuite with BeforeAndAfterAll with PrivateMethodTester with ShouldMatchers {

  var oldArch: String = _
  var oldOops: String = _

  override def beforeAll() {
    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case 
    oldArch = System.setProperty("os.arch", "amd64")
    oldOops = System.setProperty("spark.test.useCompressedOops", "true")
  }

  override def afterAll() {
    resetOrClear("os.arch", oldArch)
    resetOrClear("spark.test.useCompressedOops", oldOops)
  }

  test("simple classes") {
    expect(16)(SizeEstimator.estimate(new DummyClass1))
    expect(16)(SizeEstimator.estimate(new DummyClass2))
    expect(24)(SizeEstimator.estimate(new DummyClass3))
    expect(24)(SizeEstimator.estimate(new DummyClass4(null)))
    expect(48)(SizeEstimator.estimate(new DummyClass4(new DummyClass3)))
  }

  // NOTE: The String class definition changed in JDK 7 to exclude the int fields count and length.
  // This means that the size of strings will be lesser by 8 bytes in JDK 7 compared to JDK 6.
  // http://mail.openjdk.java.net/pipermail/core-libs-dev/2012-May/010257.html
  // Work around to check for either.
  test("strings") {
    SizeEstimator.estimate("") should (equal (48) or equal (40))
    SizeEstimator.estimate("a") should (equal (56) or equal (48))
    SizeEstimator.estimate("ab") should (equal (56) or equal (48))
    SizeEstimator.estimate("abcdefgh") should (equal(64) or equal(56))
  }

  test("primitive arrays") {
    expect(32)(SizeEstimator.estimate(new Array[Byte](10)))
    expect(40)(SizeEstimator.estimate(new Array[Char](10)))
    expect(40)(SizeEstimator.estimate(new Array[Short](10)))
    expect(56)(SizeEstimator.estimate(new Array[Int](10)))
    expect(96)(SizeEstimator.estimate(new Array[Long](10)))
    expect(56)(SizeEstimator.estimate(new Array[Float](10)))
    expect(96)(SizeEstimator.estimate(new Array[Double](10)))
    expect(4016)(SizeEstimator.estimate(new Array[Int](1000)))
    expect(8016)(SizeEstimator.estimate(new Array[Long](1000)))
  }

  test("object arrays") {
    // Arrays containing nulls should just have one pointer per element
    expect(56)(SizeEstimator.estimate(new Array[String](10)))
    expect(56)(SizeEstimator.estimate(new Array[AnyRef](10)))

    // For object arrays with non-null elements, each object should take one pointer plus
    // however many bytes that class takes. (Note that Array.fill calls the code in its
    // second parameter separately for each object, so we get distinct objects.)
    expect(216)(SizeEstimator.estimate(Array.fill(10)(new DummyClass1))) 
    expect(216)(SizeEstimator.estimate(Array.fill(10)(new DummyClass2))) 
    expect(296)(SizeEstimator.estimate(Array.fill(10)(new DummyClass3))) 
    expect(56)(SizeEstimator.estimate(Array(new DummyClass1, new DummyClass2)))

    // Past size 100, our samples 100 elements, but we should still get the right size.
    expect(28016)(SizeEstimator.estimate(Array.fill(1000)(new DummyClass3)))

    // If an array contains the *same* element many times, we should only count it once.
    val d1 = new DummyClass1
    expect(72)(SizeEstimator.estimate(Array.fill(10)(d1))) // 10 pointers plus 8-byte object
    expect(432)(SizeEstimator.estimate(Array.fill(100)(d1))) // 100 pointers plus 8-byte object

    // Same thing with huge array containing the same element many times. Note that this won't
    // return exactly 4032 because it can't tell that *all* the elements will equal the first
    // one it samples, but it should be close to that.

    // TODO: If we sample 100 elements, this should always be 4176 ?
    val estimatedSize = SizeEstimator.estimate(Array.fill(1000)(d1))
    assert(estimatedSize >= 4000, "Estimated size " + estimatedSize + " should be more than 4000")
    assert(estimatedSize <= 4200, "Estimated size " + estimatedSize + " should be less than 4100")
  }

  test("32-bit arch") {
    val arch = System.setProperty("os.arch", "x86")

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()

    expect(40)(SizeEstimator.estimate(""))
    expect(48)(SizeEstimator.estimate("a"))
    expect(48)(SizeEstimator.estimate("ab"))
    expect(56)(SizeEstimator.estimate("abcdefgh"))

    resetOrClear("os.arch", arch)
  }

  // NOTE: The String class definition changed in JDK 7 to exclude the int fields count and length.
  // This means that the size of strings will be lesser by 8 bytes in JDK 7 compared to JDK 6.
  // http://mail.openjdk.java.net/pipermail/core-libs-dev/2012-May/010257.html
  // Work around to check for either.
  test("64-bit arch with no compressed oops") {
    val arch = System.setProperty("os.arch", "amd64")
    val oops = System.setProperty("spark.test.useCompressedOops", "false")

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()

    SizeEstimator.estimate("") should (equal (64) or equal (56))
    SizeEstimator.estimate("a") should (equal (72) or equal (64))
    SizeEstimator.estimate("ab") should (equal (72) or equal (64))
    SizeEstimator.estimate("abcdefgh") should (equal (80) or equal (72))

    resetOrClear("os.arch", arch)
    resetOrClear("spark.test.useCompressedOops", oops)
  }

  def resetOrClear(prop: String, oldValue: String) {
    if (oldValue != null) {
      System.setProperty(prop, oldValue)
    } else {
      System.clearProperty(prop)
    }
  }
}
