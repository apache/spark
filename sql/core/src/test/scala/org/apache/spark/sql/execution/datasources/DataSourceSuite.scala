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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, RawLocalFileSystem}
import org.apache.spark.sql.test.SharedSparkSession

class DataSourceSuite extends SharedSparkSession {
  import TestPaths._

  test("test everything works") {
    val hadoopConf = new Configuration()
    hadoopConf.set("fs.mockFs.impl", classOf[MockFileSystem].getName)

    val allPaths = DataSource.checkAndGlobPathIfNecessary(
      Seq(
        nonGlobPath1.toString,
        nonGlobPath2.toString,
        globPath1.toString,
        globPath2.toString,
      ),
      hadoopConf,
      checkEmptyGlobPath = true,
      checkFilesExist = true
    )

    assertThrows(allPaths.equals(expectedResultPaths))
  }
}

object TestPaths {
  val nonGlobPath1: Path = new Path("mockFs:///somepath1")
  val nonGlobPath2: Path = new Path("mockFs:///somepath2")
  val globPath1: Path = new Path("mockFs:///globpath1*")
  val globPath2: Path = new Path("mockFs:///globpath2*")

  val globPath1Result1: Path = new Path("mockFs:///globpath1/path1")
  val globPath1Result2: Path = new Path("mockFs:///globpath1/path2")
  val globPath2Result1: Path = new Path("mockFs:///globpath2/path1")
  val globPath2Result2: Path = new Path("mockFs:///globpath2/path2")

  val expectedResultPaths = Seq(
    globPath1,
    globPath2,
    globPath1Result1,
    globPath1Result2,
    globPath2Result1,
    globPath2Result2,
  )

  val mockNonGlobPaths = Set(nonGlobPath1, nonGlobPath2)
  val mockGlobResults: Map[Path, Array[FileStatus]] = Map(
    globPath1 ->
      Array(
        createMockFileStatus(globPath1Result1.toString),
        createMockFileStatus(globPath1Result2.toString),
      ),
    globPath2 ->
      Array(
        createMockFileStatus(globPath2Result1.toString),
        createMockFileStatus(globPath2Result2.toString),
      ),
  )

  def createMockFileStatus(path: String): FileStatus = {
    val fileStatus = new FileStatus()
    fileStatus.setPath(new Path(path))
    fileStatus
  }
}

class MockFileSystem extends RawLocalFileSystem {
  import TestPaths._

  override def exists(f: Path): Boolean = {
    mockNonGlobPaths.contains(f)
  }

  override def globStatus(pathPattern: Path): Array[FileStatus] = {
    mockGlobResults.getOrElse(pathPattern, Array())
  }
}
