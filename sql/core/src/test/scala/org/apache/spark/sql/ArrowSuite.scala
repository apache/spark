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
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.file.json.JsonFileReader
import org.apache.arrow.vector.util.Validator

import org.apache.spark.sql.test.SharedSQLContext

class ArrowSuite extends SharedSQLContext {

  private def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).getFile
  }

  test("convert int column with null to arrow") {
    testCollect(nullInts, "test-data/arrow/null-ints.json")
  }

  test("convert string column with null to arrow") {
    val nullStringsColOnly = nullStrings.select(nullStrings.columns(1))
    testCollect(nullStringsColOnly, "test-data/arrow/null-strings.json")
  }

  /** Test that a converted DataFrame to Arrow record batch equals batch read from JSON file */
  private def testCollect(df: DataFrame, arrowFile: String) {
    val jsonFilePath = testFile(arrowFile)

    val allocator = new RootAllocator(Integer.MAX_VALUE)
    val jsonReader = new JsonFileReader(new File(jsonFilePath), allocator)

    val arrowSchema = Arrow.schemaToArrowSchema(df.schema)
    val jsonSchema = jsonReader.start()
    Validator.compareSchemas(arrowSchema, jsonSchema)

    val arrowRecordBatch = df.collectAsArrow(allocator)
    val arrowRoot = new VectorSchemaRoot(arrowSchema, allocator)
    val vectorLoader = new VectorLoader(arrowRoot)
    vectorLoader.load(arrowRecordBatch)
    val jsonRoot = jsonReader.read()

    Validator.compareVectorSchemaRoot(arrowRoot, jsonRoot)
  }
}
