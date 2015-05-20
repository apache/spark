package org.apache.spark.ml.feature

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.scalatest.FunSuite

class HashingTFSuite extends FunSuite with MLlibTestSparkContext {

  test("params") {
    val hashingTF = new HashingTF()
    ParamsSuite.checkParams(hashingTF, 3)
  }

  test("hashingTF") {

  }
}
