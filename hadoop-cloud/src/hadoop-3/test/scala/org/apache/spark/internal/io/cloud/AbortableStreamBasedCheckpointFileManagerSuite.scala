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
package org.apache.spark.internal.io.cloud

import java.io.File

import scala.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.scalatest.BeforeAndAfter

import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.cloud.abortable.AbortableFileSystem
import org.apache.spark.sql.execution.streaming.CheckpointFileManager
import org.apache.spark.sql.execution.streaming.CheckpointFileManagerTests

class AbortableStreamBasedCheckpointFileManagerSuite
  extends CheckpointFileManagerTests with Logging {

  override def withTempHadoopPath(p: Path => Unit): Unit = {
    withTempDir { f: File =>
      val basePath = new Path(AbortableFileSystem.ABORTABLE_FS_SCHEME, null, f.getAbsolutePath)
      p(basePath)
    }
  }

  override def checkLeakingCrcFiles(path: Path): Unit = { }

  override def createManager(path: Path): CheckpointFileManager = {
    val conf = new Configuration()
    conf.set(s"fs.AbstractFileSystem.${AbortableFileSystem.ABORTABLE_FS_SCHEME}.impl",
      "org.apache.spark.internal.io.cloud.abortable.AbstractAbortableFileSystem")
    new AbortableStreamBasedCheckpointFileManager(path, conf)
  }
}

@IntegrationTestSuite
class AwsS3AbortableStreamBasedCheckpointFileManagerSuite
    extends AbortableStreamBasedCheckpointFileManagerSuite with BeforeAndAfter {

  val s3aPath = Properties.envOrNone("S3A_PATH")

  val hadoopConf = new Configuration()

  var cleanup: () => Unit = () => {}

  override protected def beforeAll(): Unit = {
    assert(s3aPath.isDefined, "S3A_PATH must be defined!")
    val path = new Path(s3aPath.get)
    val fc = FileContext.getFileContext(path.toUri, hadoopConf)
    assert(!fc.util.exists(path), s"S3A_PATH ($path) should not exists!")
    fc.mkdir(path, FsPermission.getDirDefault, true)
    cleanup = () => fc.delete(path, true)
  }

  override protected def afterAll(): Unit = {
    cleanup()
  }

  override def withTempHadoopPath(p: Path => Unit): Unit = {
    p(new Path(s3aPath.get))
  }

  override def createManager(path: Path): CheckpointFileManager = {
    new AbortableStreamBasedCheckpointFileManager(path, hadoopConf)
  }
}
