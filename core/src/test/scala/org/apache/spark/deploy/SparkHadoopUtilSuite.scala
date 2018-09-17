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

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite

class SparkHadoopUtilSuite extends SparkFunSuite with Matchers {

  test("SparkHadoopUtil ServiceLoader implementation") {

    assert(SparkHadoopUtil.getInstance(Nil).getClass == classOf[SparkHadoopUtil])

    val thrown = intercept[RuntimeException] {
      SparkHadoopUtil.getInstance(List(new SparkHadoopUtil, new DummySparkHadoopUtil))
    }
    assert(thrown.getMessage == "Multiple sources found for SparkHadoopUtil " +
      "(org.apache.spark.deploy.SparkHadoopUtil, org.apache.spark.deploy.DummySparkHadoopUtil), " +
      "please specify one fully qualified class name.")

    assert(SparkHadoopUtil
      .getInstance(List(new DummySparkHadoopUtil)).getClass == classOf[DummySparkHadoopUtil])
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

class DummySparkHadoopUtil extends SparkHadoopUtil {}
