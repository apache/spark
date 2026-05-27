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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.SparkException
import org.apache.spark.sql.pipelines.utils.{PipelineTest, TestGraphRegistrationContext}
import org.apache.spark.sql.test.SharedSparkSession

class PipelineUpdateContextImplSuite extends PipelineTest with SharedSparkSession {

  test("validateStorageRoot should accept valid URIs with schemes") {
    val validStorageRoots = Seq(
      "file:///tmp/test",
      "hdfs://localhost:9000/pipelines",
      "s3a://my-bucket/pipelines",
      "abfss://container@account.dfs.core.windows.net/pipelines"
    )

    validStorageRoots.foreach(PipelineUpdateContextImpl.validateStorageRoot)
  }

  test("validateStorageRoot should reject relative paths") {
    val invalidStorageRoots = Seq(
      "relative/path",
      "./relative/path",
      "../relative/path",
      "pipelines"
    )

    invalidStorageRoots.foreach { storageRoot =>
      val exception = intercept[SparkException] {
        PipelineUpdateContextImpl.validateStorageRoot(storageRoot)
      }
      assert(exception.getCondition == "PIPELINE_STORAGE_ROOT_INVALID")
      assert(exception.getMessageParameters.get("storage_root") == storageRoot)
    }
  }

  test("validateStorageRoot should reject absolute paths without URI scheme") {
    val invalidStorageRoots = Seq(
      "/tmp/test",
      "/absolute/path",
      "/pipelines/storage"
    )

    invalidStorageRoots.foreach { storageRoot =>
      val exception = intercept[SparkException] {
        PipelineUpdateContextImpl.validateStorageRoot(storageRoot)
      }
      assert(exception.getCondition == "PIPELINE_STORAGE_ROOT_INVALID")
      assert(exception.getMessageParameters.get("storage_root") == storageRoot)
    }
  }

  test("PipelineUpdateContextImpl constructor should validate storage root") {
    val session = spark
    import session.implicits._

    class TestPipeline extends TestGraphRegistrationContext(spark) {
      registerPersistedView("test", query = dfFlowFunc(Seq(1).toDF("value")))
    }
    val graph = new TestPipeline().resolveToDataflowGraph()

    val validStorageRoot = "file:///tmp/test"
    val context = new PipelineUpdateContextImpl(
      unresolvedGraph = graph,
      eventCallback = _ => {},
      storageRoot = validStorageRoot
    )
    assert(context.storageRoot == validStorageRoot)

    val invalidStorageRoot = "/tmp/test"
    val exception = intercept[SparkException] {
      new PipelineUpdateContextImpl(
        unresolvedGraph = graph,
        eventCallback = _ => {},
        storageRoot = invalidStorageRoot
      )
    }
    assert(exception.getCondition == "PIPELINE_STORAGE_ROOT_INVALID")
    assert(exception.getMessageParameters.get("storage_root") == invalidStorageRoot)
  }
}
