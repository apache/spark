/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.shuffle

import java.io.{FileInputStream, FileOutputStream, File}

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

class ShuffleOutputCoordinatorSuite extends SparkFunSuite with BeforeAndAfterEach {

  var tempDir: File = _

  override def beforeEach(): Unit = {
    tempDir = Utils.createTempDir()
  }

  def writeFile(filename: String, data: Int): File = {
    val f = new File(tempDir, filename)
    val out = new FileOutputStream(f)
    out.write(data)
    out.close()
    f
  }

  def verifyFiles(successfulAttempt: Int): Unit = {
    (0 until 3).foreach { idx =>
      val exp = successfulAttempt* 3 + idx
      val file = new File(tempDir, s"d$idx")
      withClue(s"checking dest file $file") {
        assert(file.length === 1)
        val in = new FileInputStream(file)
        assert(in.read() === exp)
        in.close()

      }
    }
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(tempDir)
  }


  def generateAttempt(attempt: Int): Seq[(File, File)] = {
    (0 until 3).map { idx =>
      val j = attempt * 3 + idx
      writeFile(s"t$j", j) -> new File(tempDir, s"d$idx")
    }
  }

  test("move files if dest missing") {
    val firstAttempt = generateAttempt(0)
    assert(ShuffleOutputCoordinator.commitOutputs(0, 0, firstAttempt))
    verifyFiles(0)
    firstAttempt.foreach{ case (t, d) => assert(!t.exists())}

    val secondAttempt = generateAttempt(1)
    // second commit fails, and also deletes the tmp files
    assert(!ShuffleOutputCoordinator.commitOutputs(0, 0, secondAttempt))
    verifyFiles(0)
    // make sure we delete the temp files if the dest exists
    secondAttempt.foreach{ case (t, d) => assert(!t.exists())}
  }

  test("move files if dest partially missing") {
    val firstAttempt = generateAttempt(0)
    assert(ShuffleOutputCoordinator.commitOutputs(0, 0, firstAttempt))
    verifyFiles(0)
    firstAttempt.foreach{ case (t, d) => assert(!t.exists())}

    val secondAttempt = generateAttempt(1)
    firstAttempt(0)._2.delete()
    // second commit now succeeds since one destination file is missing
    assert(ShuffleOutputCoordinator.commitOutputs(0, 0, secondAttempt))
    verifyFiles(1)
    secondAttempt.foreach{ case (t, d) => assert(!t.exists())}
  }

  test("ignore missing tmp files") {
    // HashShuffle doesn't necessarily even create 0 length files for all of its output,
    // so just ignore tmp files that are missing
    val firstAttempt = generateAttempt(0) ++
      Seq(new File(tempDir, "bogus") -> new File(tempDir, "blah"))
    assert(ShuffleOutputCoordinator.commitOutputs(0, 0, firstAttempt))
    verifyFiles(0)
    assert(!new File(tempDir, "blah").exists())
    firstAttempt.foreach{ case (t, d) => assert(!t.exists())}

    // if we try again, once more with the missing tmp file, commit fails even though dest
    // is "partially missing"
    // TODO figure out right semantics, esp wrt non-determinstic data
    val secondAttempt = generateAttempt(1) ++
      Seq(new File(tempDir, "bogus") -> new File(tempDir, "blah"))
    assert(!ShuffleOutputCoordinator.commitOutputs(0, 0, secondAttempt))
    verifyFiles(0)
    assert(!new File(tempDir, "blah").exists())
    secondAttempt.foreach{ case (t, d) => assert(!t.exists())}

    // but now if we delete one of the real dest files, and try again, it goes through
    val thirdAttempt = generateAttempt(2) ++
      Seq(new File(tempDir, "bogus") -> new File(tempDir, "blah"))
    firstAttempt(0)._2.delete()
    assert(ShuffleOutputCoordinator.commitOutputs(0, 0, thirdAttempt))
    verifyFiles(2)
    assert(!new File(tempDir, "blah").exists())
    thirdAttempt.foreach{ case (t, d) => assert(!t.exists())}
  }

}
