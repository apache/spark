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
package org.apache.spark.sql.execution.datasources.xml.util

import java.nio.charset.{StandardCharsets, UnsupportedCharsetException}

import org.apache.spark.sql.test.SharedSparkSession

final class XmlFileSuite extends SharedSparkSession {

  private val resourcePrefix = "test-data/xml-resources/"
  private val booksFile = testFile(resourcePrefix + "books.xml")
  private val booksUnicodeInTagNameFile = testFile(resourcePrefix + "books-unicode-in-tag-name.xml")
  private val booksFileTag = "book"
  private val booksUnicodeFileTag = "\u66F8" // scalastyle:ignore
  private val numBooks = 12
  private val numBooksUnicodeInTagName = 3
  private val fiasHouse = testFile(resourcePrefix + "fias_house.xml")
  private val fiasRowTag = "House"
  private val numHouses = 37
  private val utf8 = StandardCharsets.UTF_8.name

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("read utf-8 encoded file") {
    val baseRDD = XmlFile.withCharset(sparkContext, booksFile, utf8, rowTag = booksFileTag)
    assert(baseRDD.count() === numBooks)
  }

  test("read file with unicode chars in row tag name") {
    val baseRDD = XmlFile.withCharset(
      sparkContext, booksUnicodeInTagNameFile, utf8, rowTag = booksUnicodeFileTag)
    assert(baseRDD.count() === numBooksUnicodeInTagName)
  }

  test("read utf-8 encoded file with empty tag") {
    val baseRDD = XmlFile.withCharset(sparkContext, fiasHouse, utf8, rowTag = fiasRowTag)
    assert(baseRDD.count() == numHouses)
    baseRDD.collect().foreach(x => assert(x.contains("/>")))
  }

  test("unsupported charset") {
    val exception = intercept[UnsupportedCharsetException] {
      XmlFile.withCharset(sparkContext, booksFile, "frylock", rowTag = booksFileTag).count()
    }
    assert(exception.getMessage.contains("frylock"))
  }

}
