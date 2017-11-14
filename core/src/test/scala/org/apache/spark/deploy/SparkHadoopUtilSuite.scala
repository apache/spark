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
import java.security.PrivilegedExceptionAction

import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.Matchers

import org.apache.spark.{LocalSparkContext, SparkFunSuite}
import org.apache.spark.util.Utils

class SparkHadoopUtilSuite extends SparkFunSuite with Matchers with LocalSparkContext{
  test("check file permission") {
    import FsAction._
    val testUser = s"user-${Random.nextInt(100)}"
    val testGroups = Array(s"group-${Random.nextInt(100)}")
    val testUgi = UserGroupInformation.createUserForTesting(testUser, testGroups)

    testUgi.doAs(new PrivilegedExceptionAction[Void] {
      override def run(): Void = {
        val sparkHadoopUtil = new SparkHadoopUtil

        // If file is owned by user and user has access permission
        var status = fileStatus(testUser, testGroups.head, READ_WRITE, READ_WRITE, NONE)
        sparkHadoopUtil.checkAccessPermission(status, READ) should be(true)
        sparkHadoopUtil.checkAccessPermission(status, WRITE) should be(true)

        // If file is owned by user but user has no access permission
        status = fileStatus(testUser, testGroups.head, NONE, READ_WRITE, NONE)
        sparkHadoopUtil.checkAccessPermission(status, READ) should be(false)
        sparkHadoopUtil.checkAccessPermission(status, WRITE) should be(false)

        val otherUser = s"test-${Random.nextInt(100)}"
        val otherGroup = s"test-${Random.nextInt(100)}"

        // If file is owned by user's group and user's group has access permission
        status = fileStatus(otherUser, testGroups.head, NONE, READ_WRITE, NONE)
        sparkHadoopUtil.checkAccessPermission(status, READ) should be(true)
        sparkHadoopUtil.checkAccessPermission(status, WRITE) should be(true)

        // If file is owned by user's group but user's group has no access permission
        status = fileStatus(otherUser, testGroups.head, READ_WRITE, NONE, NONE)
        sparkHadoopUtil.checkAccessPermission(status, READ) should be(false)
        sparkHadoopUtil.checkAccessPermission(status, WRITE) should be(false)

        // If file is owned by other user and this user has access permission
        status = fileStatus(otherUser, otherGroup, READ_WRITE, READ_WRITE, READ_WRITE)
        sparkHadoopUtil.checkAccessPermission(status, READ) should be(true)
        sparkHadoopUtil.checkAccessPermission(status, WRITE) should be(true)

        // If file is owned by other user but this user has no access permission
        status = fileStatus(otherUser, otherGroup, READ_WRITE, READ_WRITE, NONE)
        sparkHadoopUtil.checkAccessPermission(status, READ) should be(false)
        sparkHadoopUtil.checkAccessPermission(status, WRITE) should be(false)

        null
      }
    })
  }

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

      // test partial match
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"${rootDir}/dir-000[1-5]/*")) should be(Seq(
        s"file:${rootDir}/dir-0001/*",
        s"file:${rootDir}/dir-0002/*",
        s"file:${rootDir}/dir-0003/*",
        s"file:${rootDir}/dir-0004/*",
        s"file:${rootDir}/dir-0005/*"))
      // test wild cast on the leaf files
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"${rootDir}/dir-0001/*")) should be(Seq(
        s"${rootDir}/dir-0001/*"))
      // test path is not globPath
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"${rootDir}/dir-0001/part-0001")) should be(Seq(
        s"${rootDir}/dir-0001/part-0001"))
      // test the wrong wild cast
      sparkHadoopUtil.expandGlobPath(fs, new Path(s"${rootDir}/000[1-5]/*")) should be(
        Seq.empty[String])
    } finally Utils.deleteRecursively(tmpDir)
  }

  private def fileStatus(
      owner: String,
      group: String,
      userAction: FsAction,
      groupAction: FsAction,
      otherAction: FsAction): FileStatus = {
    new FileStatus(0L,
      false,
      0,
      0L,
      0L,
      0L,
      new FsPermission(userAction, groupAction, otherAction),
      owner,
      group,
      null)
  }
}
