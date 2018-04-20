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

package org.apache.spark.ml.util

import java.io.File

import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.util.Utils

/**
 * Trait that creates a temporary directory before all tests and deletes it after all.
 */
trait TempDirectory extends BeforeAndAfterAll { self: Suite =>

  private var _tempDir: File = _

  /**
   * Returns the temporary directory as a `File` instance.
   */
  protected def tempDir: File = _tempDir

  override def beforeAll(): Unit = {
    super.beforeAll()
    _tempDir = Utils.createTempDir(namePrefix = this.getClass.getName)
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(_tempDir)
    } finally {
      super.afterAll()
    }
  }
}
