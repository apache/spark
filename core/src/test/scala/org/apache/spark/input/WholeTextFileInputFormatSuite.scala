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

package org.apache.spark.input

import java.io.{DataOutputStream, File, FileOutputStream}

import scala.collection.immutable.IndexedSeq

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Tests the correctness of
 * [[org.apache.spark.input.WholeTextFileInputFormat WholeTextFileInputFormat]]. A temporary
 * directory containing files is created as fake input which is deleted in the end.
 */
class WholeTextFileInputFormatSuite extends SparkFunSuite with BeforeAndAfterAll with Logging {
  private var sc: SparkContext = _

  override def beforeAll() {
    super.beforeAll()
    val conf = new SparkConf()
    sc = new SparkContext("local", "test", conf)
  }

  override def afterAll() {
    try {
      sc.stop()
    } finally {
      super.afterAll()
    }
  }

  private def createNativeFile(inputDir: File, fileName: String, contents: Array[Byte],
                               compress: Boolean) = {
    val path = s"${inputDir.toString}/$fileName"
    val out = new DataOutputStream(new FileOutputStream(path))
    out.write(contents, 0, contents.length)
    out.close()
  }

  test("for small files minimum split size per node and per rack should be less than or equal to " +
    "maximum split size.") {
    withTempDir { dir =>
      logInfo(s"Local disk address is ${dir.toString}.")

      // Set the minsize per node and rack to be larger than the size of the input file.
      sc.hadoopConfiguration.setLong(
        "mapreduce.input.fileinputformat.split.minsize.per.node", 123456)
      sc.hadoopConfiguration.setLong(
        "mapreduce.input.fileinputformat.split.minsize.per.rack", 123456)

      WholeTextFileInputFormatSuite.files.foreach { case (filename, contents) =>
        createNativeFile(dir, filename, contents, false)
      }
      // ensure spark job runs successfully without exceptions from the CombineFileInputFormat
      assert(sc.wholeTextFiles(dir.toString).count == 3)
    }
  }
}

/**
 * Files to be tested are defined here.
 */
object WholeTextFileInputFormatSuite {
  private val testWords: IndexedSeq[Byte] = "Spark is easy to use.\n".map(_.toByte)

  private val fileNames = Array("part-00000", "part-00001", "part-00002")
  private val fileLengths = Array(10, 100, 1000)

  private val files = fileLengths.zip(fileNames).map { case (upperBound, filename) =>
    filename -> Stream.continually(testWords.toList.toStream).flatten.take(upperBound).toArray
  }.toMap
}
