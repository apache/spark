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

package org.apache.spark.mllib

import org.apache.spark.mllib.MLContext._
import org.apache.spark.mllib.util.LocalSparkContext
import org.scalatest.FunSuite
import com.google.common.io.Files
import java.io.File
import com.google.common.base.Charsets
import org.apache.spark.mllib.linalg.Vectors

class MLContextSuite extends FunSuite with LocalSparkContext {
  test("libSVMFile") {
    val lines =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val tempDir = Files.createTempDir()
    val file = new File(tempDir.getPath, "part-00000")
    Files.write(lines, file, Charsets.US_ASCII)
    val points = sc.libSVMFile(tempDir.toURI.toString, 6).collect()
    assert(points.length === 2)
    assert(points(0).label === 1.0)
    assert(points(0).features === Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
    assert(points(1).label === 0.0)
    assert(points(1).features === Vectors.sparse(6, Seq((1, 4.0), (3, 5.0), (5, 6.0))))
    try {
      file.delete()
      tempDir.delete()
    } catch {
      case t: Throwable =>
    }
  }
}
