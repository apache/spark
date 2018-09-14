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

package org.apache.spark.deploy

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.Matchers

import org.apache.spark.{LocalSparkContext, SparkFunSuite}
import org.apache.spark.util.Utils

class SparkHadoopUtilSuite extends SparkFunSuite with Matchers with LocalSparkContext {
  test("test expanding glob path") {
    val tmpDir = Utils.createTempDir()
    val rootDir = tmpDir.getCanonicalPath
    try {
      // Prepare nested dir env: /tmpPath/dir-${dirIndex}/part-${fileIndex}
      for (i <- 1 to 10) {
        val dirStr = new StringFormat(i.toString)
        val curDir = rootDir + dirStr.formatted("/dir-%4s").replaceAll(" ", "0")
        for (j <- 1 to 10) {
          val fileStr = new StringFormat(j.toString)
          val file = new File(curDir, fileStr.formatted("/part-%4s").replaceAll(" ", "0"))

          file.getParentFile.exists() || file.getParentFile.mkdirs()
          file.createNewFile()
        }
      }
      val sparkHadoopUtil = new SparkHadoopUtil
      val fs = FileSystem.getLocal(new Configuration())

      // when we set threshold to 5, just expand the dir-000[1-5]
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"$rootDir/dir-000[1-5]/*"), 5)
        .sortWith(_.compareTo(_) < 0) should be(Seq(
          new Path(s"file:$rootDir/dir-0001/*"),
          new Path(s"file:$rootDir/dir-0002/*"),
          new Path(s"file:$rootDir/dir-0003/*"),
          new Path(s"file:$rootDir/dir-0004/*"),
          new Path(s"file:$rootDir/dir-0005/*")))

      // when we set threshold to 10, we'll get all 50 files in whole pattern
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"$rootDir/dir-000[1-5]/*"), 10)
        .sortWith(_.compareTo(_) < 0).size should be(50)

      // test wild cast on the leaf files
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"$rootDir/dir-0001/*"), 5).size should be(10)

      // test path is not globPath
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"$rootDir/dir-0001/part-0001"), 10) should
        be(Seq(new Path(s"$rootDir/dir-0001/part-0001")))

      // test the wrong wild cast
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"$rootDir/000[1-5]/*"), 10) should be(
        Seq.empty[Path])
    } finally Utils.deleteRecursively(tmpDir)
  }
}
