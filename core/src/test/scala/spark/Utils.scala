package spark

import org.scalatest.FunSuite


class UtilsSuite extends FunSuite {

  test("sizeWithSuffix") {
    assert(Utils.sizeWithSuffix(10) === "10.0B")
    assert(Utils.sizeWithSuffix(1500) === "1500.0B")
    assert(Utils.sizeWithSuffix(2000000) === "1953.1KB")
    assert(Utils.sizeWithSuffix(2097152) === "2.0MB")
    assert(Utils.sizeWithSuffix(2306867) === "2.2MB")
    assert(Utils.sizeWithSuffix(5368709120L) === "5.0GB")
  }

}

