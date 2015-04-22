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

import org.apache.spark.ml.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.MetadataBuilder
import org.scalatest.FunSuite

private[ml] object TestingUtils extends FunSuite {

  /**
   * Test whether unrelated metadata are preserved for this transformer.
   * This attaches extra metadata to a column, transforms the column, and check to ensure the
   * extra metadata have not changed.
   * @param data  Input dataset
   * @param transformer  Transformer to test
   * @param inputCol  Unique input column for Transformer.  This must be the ONLY input column.
   * @param outputCol  Output column to test for metadata presence.
   */
  def testPreserveMetadata(
      data: DataFrame,
      transformer: Transformer,
      inputCol: String,
      outputCol: String): Unit = {
    // Create some fake metadata
    val origMetadata = data.schema(inputCol).metadata
    val metaKey = "__testPreserveMetadata__fake_key"
    val metaValue = 12345
    assert(!origMetadata.contains(metaKey),
      s"Unit test with testPreserveMetadata will fail since metadata key was present: $metaKey")
    val newMetadata =
      new MetadataBuilder().withMetadata(origMetadata).putLong(metaKey, metaValue).build()
    // Add metadata to the inputCol
    val withMetadata = data.select(data(inputCol).as(inputCol, newMetadata))
    // Transform, and ensure extra metadata was not affected
    val transformed = transformer.transform(withMetadata)
    val transMetadata = transformed.schema(outputCol).metadata
    assert(transMetadata.contains(metaKey),
      "Unit test with testPreserveMetadata failed; extra metadata key was not present.")
    assert(transMetadata.getLong(metaKey) === metaValue,
      "Unit test with testPreserveMetadata failed; extra metadata value was wrong." +
        s" Expected $metaValue but found ${transMetadata.getLong(metaKey)}")
  }
}
