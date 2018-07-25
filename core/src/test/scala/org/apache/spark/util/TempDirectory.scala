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

package org.apache.spark.util

import java.io.File

import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

/**
 * Trait that creates a temporary directory before each test and deletes it after the test.
 */
trait TempDirectory extends BeforeAndAfterEach { self: Suite =>

  private var _tempDir: File = _

  /**
   * Returns the temporary directory as a `File` instance.
   */
  protected def tempDir: File = _tempDir

  override def beforeEach(): Unit = {
    super.beforeEach()
    _tempDir = Utils.createTempDir(namePrefix = this.getClass.getName)
  }

  override def afterEach(): Unit = {
    try {
      Utils.deleteRecursively(_tempDir)
    } finally {
      super.afterEach()
    }
  }
}
