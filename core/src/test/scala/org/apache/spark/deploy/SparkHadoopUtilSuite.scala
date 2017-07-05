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

import java.security.PrivilegedExceptionAction

import scala.util.Random

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.security.UserGroupInformation
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class SparkHadoopUtilSuite extends SparkFunSuite with Matchers {
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
