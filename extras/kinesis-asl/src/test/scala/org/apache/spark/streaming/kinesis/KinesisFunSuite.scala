package org.apache.spark.streaming.kinesis

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.SparkFunSuite


abstract class KinesisFunSuite extends SparkFunSuite with BeforeAndAfterAll {
  import KinesisTestUtils._

  def testOrIgnore(testName: String)(testBody: => Unit) {
    if (shouldRunTests) {
      test(testName)(testBody)
    } else {
      ignore(s"$testName [enable by setting env var $envVarName=1]")(testBody)
    }
  }
}