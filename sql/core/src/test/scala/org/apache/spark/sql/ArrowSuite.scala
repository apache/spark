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
package org.apache.spark.sql

import java.io.File

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.tools.Integration
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.file.json.JsonFileReader

import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}

class ArrowSuite extends QueryTest with SharedSQLContext with SQLTestUtils {
  import testImplicits._
  private val nullIntsFile = "test-data/arrowNullInts.json"

  private def testFile(fileName: String): String = {
    // TODO: Copied from CSVSuite, find a better way to read test files
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString.substring(5)
  }

  test("convert int column with null to arrow") {
    val df = nullInts
    val jsonFilePath = testFile(nullIntsFile)

    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val jsonReader = new JsonFileReader(new File(jsonFilePath), allocator)

    val arrowSchema = df.schemaToArrowSchema(df.schema)
    val jsonSchema = jsonReader.start()
    // TODO - requires changing to public API in arrow, will be addressed in ARROW-411
    //Integration.compareSchemas(arrowSchema, jsonSchema)

    val arrowRecordBatch = df.collectAsArrow(allocator)
    val arrowRoot = new VectorSchemaRoot(arrowSchema, allocator)
    val vectorLoader = new VectorLoader(arrowRoot)
    vectorLoader.load(arrowRecordBatch)
    val jsonRoot = jsonReader.read()

    // TODO - requires changing to public API in arrow, will be addressed in ARROW-411
    //Integration.compare(arrowRoot, jsonRoot)
  }
}
