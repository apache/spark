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

package org.apache.spark.sql.catalyst.catalog

import java.io.File
import java.nio.file.Files

import org.apache.spark.SparkException

/** Test suite for the [[InMemoryCatalog]]. */
class InMemoryCatalogSuite extends ExternalCatalogSuite {

  protected override val utils: CatalogTestUtils = new CatalogTestUtils {
    override val tableInputFormat: String = "org.apache.park.SequenceFileInputFormat"
    override val tableOutputFormat: String = "org.apache.park.SequenceFileOutputFormat"
    override val defaultProvider: String = "parquet"
    override def newEmptyCatalog(): ExternalCatalog = new InMemoryCatalog
  }

  test("createDatabase throws UNABLE_TO_CREATE_DATABASE_DIRECTORY when mkdirs fails") {
    withTempDir { parentFile =>
      // A path nested under an existing regular file: the filesystem cannot create it as a
      // directory, so `fs.mkdirs` throws an IOException.
      val blockingFile = new File(parentFile, "not-a-directory")
      Files.createFile(blockingFile.toPath)
      val dbLocation = new File(blockingFile, "db_dir").toURI

      val catalog = new InMemoryCatalog
      val db = CatalogDatabase("unreachable_db", "db", dbLocation, Map.empty)
      checkError(
        exception = intercept[SparkException] {
          catalog.createDatabase(db, ignoreIfExists = false)
        },
        condition = "UNABLE_TO_CREATE_DATABASE_DIRECTORY",
        parameters = Map(
          "name" -> "unreachable_db",
          "locationUri" -> dbLocation.toString))
    }
  }

}
