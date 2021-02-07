package org.apache.spark.ml.tuning

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.TestParams
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

class ParamRandomBuilderSuite extends SparkFunSuite with ScalaCheckDrivenPropertyChecks with Matchers {

  val solver = new TestParams()
//  import solver.{inputCol, maxIter}

  test("random params") {
    // TODO
  }

}
