
package org.apache.spark.deploy

import java.io.File
import java.nio.file.Files

import org.apache.spark.{SparkFunSuite, SparkUserAppException}

class RRunnerSuite extends SparkFunSuite {

  test("RRunner main") {
    val tempRFile = File.createTempFile("test", ".R")
    Files.write(tempRFile.toPath,
      s"""
        quit()
      """.stripMargin.getBytes("utf8"))
    RRunner.main(Array(tempRFile.toURI.toString))

    val exception = intercept[SparkUserAppException] { RRunner.main(Array("nonexistent.R")) }
    assert(exception.exitCode === 2)
    assert(exception.message.nonEmpty)
    assert(exception.message.get.contains(
      "Fatal error: cannot open file 'nonexistent.R': No such file or directory"))
  }
}
