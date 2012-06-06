package spark

import org.scalatest.FunSuite

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

class SizeEstimatorSuite extends FunSuite {
  test("simple classes") {
    expect(8)(SizeEstimator.estimate(new DummyClass1))
    expect(12)(SizeEstimator.estimate(new DummyClass2))
    expect(20)(SizeEstimator.estimate(new DummyClass3))
    expect(16)(SizeEstimator.estimate(new DummyClass4(null)))
    expect(36)(SizeEstimator.estimate(new DummyClass4(new DummyClass3)))
  }

  test("strings") {
    expect(24)(SizeEstimator.estimate(""))
    expect(26)(SizeEstimator.estimate("a"))
    expect(28)(SizeEstimator.estimate("ab"))
    expect(40)(SizeEstimator.estimate("abcdefgh"))
  }

  test("primitive arrays") {
    expect(10)(SizeEstimator.estimate(new Array[Byte](10)))
    expect(20)(SizeEstimator.estimate(new Array[Char](10)))
    expect(20)(SizeEstimator.estimate(new Array[Short](10)))
    expect(40)(SizeEstimator.estimate(new Array[Int](10)))
    expect(80)(SizeEstimator.estimate(new Array[Long](10)))
    expect(40)(SizeEstimator.estimate(new Array[Float](10)))
    expect(80)(SizeEstimator.estimate(new Array[Double](10)))
    expect(4000)(SizeEstimator.estimate(new Array[Int](1000)))
    expect(8000)(SizeEstimator.estimate(new Array[Long](1000)))
  }

  test("object arrays") {
    // Arrays containing nulls should just have one pointer per element
    expect(40)(SizeEstimator.estimate(new Array[String](10)))
    expect(40)(SizeEstimator.estimate(new Array[AnyRef](10)))

    // For object arrays with non-null elements, each object should take one pointer plus
    // however many bytes that class takes. (Note that Array.fill calls the code in its
    // second parameter separately for each object, so we get distinct objects.)
    expect(120)(SizeEstimator.estimate(Array.fill(10)(new DummyClass1))) 
    expect(160)(SizeEstimator.estimate(Array.fill(10)(new DummyClass2))) 
    expect(240)(SizeEstimator.estimate(Array.fill(10)(new DummyClass3))) 
    expect(12 + 16)(SizeEstimator.estimate(Array(new DummyClass1, new DummyClass2)))

    // Past size 100, our samples 100 elements, but we should still get the right size.
    expect(24000)(SizeEstimator.estimate(Array.fill(1000)(new DummyClass3)))

    // If an array contains the *same* element many times, we should only count it once.
    val d1 = new DummyClass1
    expect(48)(SizeEstimator.estimate(Array.fill(10)(d1))) // 10 pointers plus 8-byte object
    expect(408)(SizeEstimator.estimate(Array.fill(100)(d1))) // 100 pointers plus 8-byte object

    // Same thing with huge array containing the same element many times. Note that this won't
    // return exactly 4008 because it can't tell that *all* the elements will equal the first
    // one it samples, but it should be close to that.
    val estimatedSize = SizeEstimator.estimate(Array.fill(1000)(d1))
    assert(estimatedSize >= 4000, "Estimated size " + estimatedSize + " should be more than 4000")
    assert(estimatedSize <= 4100, "Estimated size " + estimatedSize + " should be less than 4100")
  }
}

