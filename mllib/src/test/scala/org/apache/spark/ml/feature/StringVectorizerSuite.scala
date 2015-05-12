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

package org.apache.spark.ml.feature

import org.scalatest.FunSuite

import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

class StringVectorizerSuite extends FunSuite with MLlibTestSparkContext {
  private var sqlContext: SQLContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
  }

  def stringIndexed(): DataFrame = {
    val data = sc.parallelize(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")), 2)
    val df = sqlContext.createDataFrame(data).toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    indexer.transform(df)
  }

  test("StringVectorizer includeFirst = true") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    val expected = encoder.transform(transformed)
 
    val vectorizer = new StringVectorizer()
      .setInputCol("label")
      .setOutputCol("labelVectorized")
      .fit(expected)

    val output = vectorizer.transform(expected).select("labelVec", "labelVectorized").collect()
    output.foreach(row => assert(row(0) === row(1)))
  }
 
  test("StringVectorizer includeFirst = false") {
    val transformed = stringIndexed()
    val encoder = new OneHotEncoder()
      .setIncludeFirst(false)
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    val expected = encoder.transform(transformed)
 
    val vectorizer = new StringVectorizer()
      .setIncludeFirst(false)
      .setInputCol("label")
      .setOutputCol("labelVectorized")
      .fit(expected)

    val output = vectorizer.transform(expected).select("labelVec", "labelVectorized").collect()
    output.foreach(row => assert(row(0) === row(1)))
  }

  test("StringVectorizer with a numeric input column") {
    val data = sc.parallelize(Seq((0, 100), (1, 200), (2, 300), (3, 100), (4, 100), (5, 300)), 2)
    val df = sqlContext.createDataFrame(data).toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    val transformed = indexer.transform(df)
 
    val encoder = new OneHotEncoder()
      .setInputCol("labelIndex")
      .setOutputCol("labelVec")
    val expected = encoder.transform(transformed)
 
    val vectorizer = new StringVectorizer()
      .setInputCol("label")
      .setOutputCol("labelVectorized")
      .fit(expected)

    val output = vectorizer.transform(expected).select("labelVec", "labelVectorized").collect()
    output.foreach(row => assert(row(0) === row(1)))
  }
}
