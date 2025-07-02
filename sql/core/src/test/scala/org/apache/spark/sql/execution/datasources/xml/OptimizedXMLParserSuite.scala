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

package org.apache.spark.sql.execution.datasources.xml

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{DropMalformedMode, PermissiveMode}
import org.apache.spark.sql.functions.{col, variant_get}
import org.apache.spark.sql.internal.SQLConf

class OptimizedXMLParserSuite extends XmlSuite with XmlVariantTests {
  override protected def sparkConf = {
    val conf = super.sparkConf
    conf.set(SQLConf.ENABLE_OPTIMIZED_XML_PARSER.key, "true")
    conf
  }

  override def excluded: Seq[String] = super.excluded ++ Seq(
    // XSD validation is not supported in optimized XML parser
    "test XSD validation with validation error",
    "test XSD validation with addFile() with validation error",
    "DSL: test XSD validation",
    // Malformed recording handling is slightly different in optimized XML parser
    "DSL test for parsing a malformed XML file",
    "DSL test for permissive mode for corrupt records",
    "DSL test with malformed attributes",
    "DSL test for dropping malformed rows",
    "DSL: handle malformed record in singleVariantColumn mode",
    // No valid row will be found in `unclosed_tag.xml` by the OptimizedXMLTokenizer
    "test FAILFAST with unclosed tag",
    // The file `fias_house.xml` can't be directly read by the XMLEventReader in the
    // optimized XML parser
    "read utf-8 encoded file with empty tag",
    "read utf-8 encoded file with empty tag 2"
  )

  test("DSL test with malformed attributes - optimized XML parser") {
    val results = spark.read
      .option("mode", DropMalformedMode.name)
      .option("rowTag", "book")
      .xml(getTestResourcePath(resDir + "books-malformed-attributes.xml"))
      .collect()

    // In optimized XML parser, the rest of the XML file content is skipped after the first
    // malformed record. Because the first record is malformed, no records are returned.
    assert(results.length === 0)
  }

  test("DSL test for parsing a malformed XML file - optimized XML parser") {
    val results = spark.read
      .option("rowTag", "ROW")
      .option("mode", DropMalformedMode.name)
      .xml(getTestResourcePath(resDir + "cars-malformed.xml"))

    // No record because the first record is malformed and thus the whole file is dropped
    assert(results.count() === 0)
  }

  test("DSL test for permissive mode for corrupt records - optimized XML parser") {
    val carsDf = spark.read
      .option("rowTag", "ROW")
      .option("mode", PermissiveMode.name)
      .option("columnNameOfCorruptRecord", "_malformed_records")
      .xml(getTestResourcePath(resDir + "cars-malformed.xml"))
    val cars = carsDf.collect()

    // The full XML file is put into the `_malformed_records` column because the first record is
    // malformed
    assert(cars.length === 1)
    assert(carsDf.cache().select("_malformed_records").first().get(0).toString.nonEmpty)
  }

  test("DSL: handle malformed record in singleVariantColumn mode - optimized XML parser") {
    // FAILFAST mode
    checkError(
      exception = intercept[SparkException] {
        createDSLDataFrame(
          fileName = "cars-malformed.xml",
          singleVariantColumn = Some("var"),
          extraOptions = Map("mode" -> "FAILFAST")
        ).collect()
      }.getCause.asInstanceOf[SparkException],
      condition = "MALFORMED_RECORD_IN_PARSING.WITHOUT_SUGGESTION",
      parameters = Map("badRecord" -> "[null]", "failFastMode" -> "FAILFAST")
    )

    // PERMISSIVE mode
    val df = createDSLDataFrame(
      fileName = "cars-malformed.xml",
      singleVariantColumn = Some("var"),
      extraOptions = Map("mode" -> "PERMISSIVE")
    )
    checkAnswer(
      df.select(variant_get(col("var"), "$.year", "int")),
      Seq(Row(null))
    )

    // DROPMALFORMED mode
    val df2 = createDSLDataFrame(
      fileName = "cars-malformed.xml",
      singleVariantColumn = Some("var"),
      extraOptions = Map("mode" -> "DROPMALFORMED")
    )
    checkAnswer(
      df2.select(variant_get(col("var"), "$.year", "int")),
      Seq.empty
    )
  }
}
