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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class StringIndexerSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new StringIndexer)
    val model = new StringIndexerModel("indexer", Array("a", "b"))
    val modelWithoutUid = new StringIndexerModel(Array("a", "b"))
    ParamsSuite.checkParams(model)
    ParamsSuite.checkParams(modelWithoutUid)
  }

  test("StringIndexer") {
    val data = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(indexer)

    val transformed = indexer.transform(df)
    val attr = Attribute.fromStructField(transformed.schema("labelIndex"))
      .asInstanceOf[NominalAttribute]
    assert(attr.values.get === Array("a", "c", "b"))
    val output = transformed.select("id", "labelIndex").rdd.map { r =>
      (r.getInt(0), r.getDouble(1))
    }.collect().toSet
    // a -> 0, b -> 2, c -> 1
    val expected = Set((0, 0.0), (1, 2.0), (2, 1.0), (3, 0.0), (4, 0.0), (5, 1.0))
    assert(output === expected)
  }

  test("StringIndexerUnseen") {
    val data = Seq((0, "a"), (1, "b"), (4, "b"))
    val data2 = Seq((0, "a"), (1, "b"), (2, "c"))
    val df = data.toDF("id", "label")
    val df2 = data2.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    // Verify we throw by default with unseen values
    intercept[SparkException] {
      indexer.transform(df2).collect()
    }
    val indexerSkipInvalid = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .setHandleInvalid("skip")
      .fit(df)
    // Verify that we skip the c record
    val transformed = indexerSkipInvalid.transform(df2)
    val attr = Attribute.fromStructField(transformed.schema("labelIndex"))
      .asInstanceOf[NominalAttribute]
    assert(attr.values.get === Array("b", "a"))
    val output = transformed.select("id", "labelIndex").rdd.map { r =>
      (r.getInt(0), r.getDouble(1))
    }.collect().toSet
    // a -> 1, b -> 0
    val expected = Set((0, 1.0), (1, 0.0))
    assert(output === expected)
  }

  test("StringIndexer with a numeric input column") {
    val data = Seq((0, 100), (1, 200), (2, 300), (3, 100), (4, 100), (5, 300))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    val transformed = indexer.transform(df)
    val attr = Attribute.fromStructField(transformed.schema("labelIndex"))
      .asInstanceOf[NominalAttribute]
    assert(attr.values.get === Array("100", "300", "200"))
    val output = transformed.select("id", "labelIndex").rdd.map { r =>
      (r.getInt(0), r.getDouble(1))
    }.collect().toSet
    // 100 -> 0, 200 -> 2, 300 -> 1
    val expected = Set((0, 0.0), (1, 2.0), (2, 1.0), (3, 0.0), (4, 0.0), (5, 1.0))
    assert(output === expected)
  }

  test("StringIndexerModel should keep silent if the input column does not exist.") {
    val indexerModel = new StringIndexerModel("indexer", Array("a", "b", "c"))
      .setInputCol("label")
      .setOutputCol("labelIndex")
    val df = spark.range(0L, 10L).toDF()
    assert(indexerModel.transform(df).collect().toSet === df.collect().toSet)
  }

  test("StringIndexerModel can't overwrite output column") {
    val df = Seq((1, 2), (3, 4)).toDF("input", "output")
    intercept[IllegalArgumentException] {
      new StringIndexer()
        .setInputCol("input")
        .setOutputCol("output")
        .fit(df)
    }

    val indexer = new StringIndexer()
      .setInputCol("input")
      .setOutputCol("indexedInput")
      .fit(df)

    intercept[IllegalArgumentException] {
      indexer.setOutputCol("output").transform(df)
    }
  }

  test("StringIndexer read/write") {
    val t = new StringIndexer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setHandleInvalid("skip")
    testDefaultReadWrite(t)
  }

  test("StringIndexerModel read/write") {
    val instance = new StringIndexerModel("myStringIndexerModel", Array("a", "b", "c"))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setHandleInvalid("skip")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.labels === instance.labels)
  }

  test("IndexToString params") {
    val idxToStr = new IndexToString()
    ParamsSuite.checkParams(idxToStr)
  }

  test("IndexToString.transform") {
    val labels = Array("a", "b", "c")
    val df0 = Seq((0, "a"), (1, "b"), (2, "c"), (0, "a")).toDF("index", "expected")

    val idxToStr0 = new IndexToString()
      .setInputCol("index")
      .setOutputCol("actual")
      .setLabels(labels)
    idxToStr0.transform(df0).select("actual", "expected").collect().foreach {
      case Row(actual, expected) =>
        assert(actual === expected)
    }

    val attr = NominalAttribute.defaultAttr.withValues(labels)
    val df1 = df0.select(col("index").as("indexWithAttr", attr.toMetadata()), col("expected"))

    val idxToStr1 = new IndexToString()
      .setInputCol("indexWithAttr")
      .setOutputCol("actual")
    idxToStr1.transform(df1).select("actual", "expected").collect().foreach {
      case Row(actual, expected) =>
        assert(actual === expected)
    }
  }

  test("StringIndexer, IndexToString are inverses") {
    val data = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    val transformed = indexer.transform(df)
    val idx2str = new IndexToString()
      .setInputCol("labelIndex")
      .setOutputCol("sameLabel")
      .setLabels(indexer.labels)
    idx2str.transform(transformed).select("label", "sameLabel").collect().foreach {
      case Row(a: String, b: String) =>
        assert(a === b)
    }
  }

  test("IndexToString.transformSchema (SPARK-10573)") {
    val idxToStr = new IndexToString().setInputCol("input").setOutputCol("output")
    val inSchema = StructType(Seq(StructField("input", DoubleType)))
    val outSchema = idxToStr.transformSchema(inSchema)
    assert(outSchema("output").dataType === StringType)
  }

  test("IndexToString read/write") {
    val t = new IndexToString()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setLabels(Array("a", "b", "c"))
    testDefaultReadWrite(t)
  }

  test("StringIndexer metadata") {
    val data = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    val transformed = indexer.transform(df)
    val attrs =
      NominalAttribute.decodeStructField(transformed.schema("labelIndex"), preserveName = true)
    assert(attrs.name.nonEmpty && attrs.name.get === "labelIndex")
  }
}
