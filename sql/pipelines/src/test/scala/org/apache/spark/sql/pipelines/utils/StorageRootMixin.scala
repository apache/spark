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

package org.apache.spark.sql.pipelines.utils


import java.io.File
import java.nio.file.Files

import org.scalatest.{BeforeAndAfterEach, Suite}

import org.apache.spark.util.Utils

/**
 * A mixin trait for tests that need a temporary directory as the storage root for pipelines.
 * This trait creates a temporary directory before each test and deletes it after each test.
 * The path to the temporary directory is available via the `storageRoot` variable.
 */
trait StorageRootMixin extends BeforeAndAfterEach { self: Suite =>

  /** A temporary directory created as the pipeline storage root for each test. */
  protected var storageRoot: String = _

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    storageRoot =
      s"file://${Files.createTempDirectory(getClass.getSimpleName).normalize.toString}"
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    Utils.deleteRecursively(new File(storageRoot))
  }
}
