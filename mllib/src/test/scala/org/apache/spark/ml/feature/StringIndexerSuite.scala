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

import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class StringIndexerSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new StringIndexer)
    val model = new StringIndexerModel("indexer", Array(Array("a", "b")))
    val modelWithoutUid = new StringIndexerModel(Array("a", "b"))
    ParamsSuite.checkParams(model)
    ParamsSuite.checkParams(modelWithoutUid)
  }

  test("params: input/output columns") {
    val stringIndexerSingleCol = new StringIndexer()
      .setInputCol("in").setOutputCol("out")
    val inOutCols1 = stringIndexerSingleCol.getInOutCols()
    assert(inOutCols1._1 === Array("in"))
    assert(inOutCols1._2 === Array("out"))

    val stringIndexerMultiCol = new StringIndexer()
      .setInputCols(Array("in1", "in2")).setOutputCols(Array("out1", "out2"))
    val inOutCols2 = stringIndexerMultiCol.getInOutCols()
    assert(inOutCols2._1 === Array("in1", "in2"))
    assert(inOutCols2._2 === Array("out1", "out2"))


    val df = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c")).toDF("id", "label")

    intercept[IllegalArgumentException] {
      new StringIndexer().setInputCol("in").setOutputCols(Array("out1", "out2")).fit(df)
    }
    intercept[IllegalArgumentException] {
      new StringIndexer().setInputCols(Array("in1", "in2")).setOutputCol("out1").fit(df)
    }
    intercept[IllegalArgumentException] {
      new StringIndexer().setInputCols(Array("in1", "in2"))
        .setOutputCols(Array("out1", "out2", "out3"))
        .fit(df)
    }
    intercept[IllegalArgumentException] {
      new StringIndexer().setInputCols(Array("in1", "in2"))
        .setOutputCols(Array("out1", "out1"))
        .fit(df)
    }
  }

  test("StringIndexer") {
    val data = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
    val indexerModel = indexer.fit(df)
    MLTestingUtils.checkCopyAndUids(indexer, indexerModel)
    // a -> 0, b -> 2, c -> 1
    val expected = Seq(
      (0, 0.0),
      (1, 2.0),
      (2, 1.0),
      (3, 0.0),
      (4, 0.0),
      (5, 1.0)
    ).toDF("id", "labelIndex")

    testTransformerByGlobalCheckFunc[(Int, String)](df, indexerModel, "id", "labelIndex") { rows =>
      val attr = Attribute.fromStructField(rows.head.schema("labelIndex"))
        .asInstanceOf[NominalAttribute]
      assert(attr.values.get === Array("a", "c", "b"))
      assert(rows.seq === expected.collect().toSeq)
    }
  }

  test("StringIndexer.transformSchema)") {
    val idxToStr = new StringIndexer().setInputCol("input").setOutputCol("output")
    val inSchema = StructType(Seq(StructField("input", StringType)))
    val outSchema = idxToStr.transformSchema(inSchema)
    assert(outSchema("output").dataType === DoubleType)
  }

  test("StringIndexer.transformSchema multi col") {
    val idxToStr = new StringIndexer().setInputCols(Array("input", "input2")).
      setOutputCols(Array("output", "output2"))
    val inSchema = StructType(Seq(StructField("input", StringType),
      StructField("input2", StringType)))
    val outSchema = idxToStr.transformSchema(inSchema)
    assert(outSchema("output").dataType === DoubleType)
    assert(outSchema("output2").dataType === DoubleType)
  }

  test("StringIndexerUnseen") {
    val data = Seq((0, "a"), (1, "b"), (4, "b"))
    val data2 = Seq((0, "a"), (1, "b"), (2, "c"), (3, "d"))
    val df = data.toDF("id", "label")
    val df2 = data2.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)

    // Verify we throw by default with unseen values
    testTransformerByInterceptingException[(Int, String)](
      df2,
      indexer,
      "Unseen label:",
      "labelIndex")

    // Verify that we skip the c record
    // a -> 1, b -> 0
    indexer.setHandleInvalid("skip")

    val expectedSkip = Seq((0, 1.0), (1, 0.0)).toDF()
    testTransformerByGlobalCheckFunc[(Int, String)](df2, indexer, "id", "labelIndex") { rows =>
      val attrSkip = Attribute.fromStructField(rows.head.schema("labelIndex"))
        .asInstanceOf[NominalAttribute]
      assert(attrSkip.values.get === Array("b", "a"))
      assert(rows.seq === expectedSkip.collect().toSeq)
    }

    indexer.setHandleInvalid("keep")

    // a -> 1, b -> 0, c -> 2, d -> 3
    val expectedKeep = Seq((0, 1.0), (1, 0.0), (2, 2.0), (3, 2.0)).toDF()

    // Verify that we keep the unseen records
    testTransformerByGlobalCheckFunc[(Int, String)](df2, indexer, "id", "labelIndex") { rows =>
      val attrKeep = Attribute.fromStructField(rows.head.schema("labelIndex"))
        .asInstanceOf[NominalAttribute]
      assert(attrKeep.values.get === Array("b", "a", "__unknown"))
      assert(rows === expectedKeep.collect().toSeq)
    }
  }

  test("StringIndexer with a numeric input column") {
    val data = Seq((0, 100), (1, 200), (2, 300), (3, 100), (4, 100), (5, 300))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    // 100 -> 0, 200 -> 2, 300 -> 1
    val expected = Seq((0, 0.0), (1, 2.0), (2, 1.0), (3, 0.0), (4, 0.0), (5, 1.0)).toDF()
    testTransformerByGlobalCheckFunc[(Int, String)](df, indexer, "id", "labelIndex") { rows =>
      val attr = Attribute.fromStructField(rows.head.schema("labelIndex"))
        .asInstanceOf[NominalAttribute]
      assert(attr.values.get === Array("100", "300", "200"))
      assert(rows === expected.collect().toSeq)
    }
  }

  test("StringIndexer with NULLs") {
    val data: Seq[(Int, String)] = Seq((0, "a"), (1, "b"), (2, "b"), (3, null))
    val data2: Seq[(Int, String)] = Seq((0, "a"), (1, "b"), (3, null))
    val df = data.toDF("id", "label")
    val df2 = data2.toDF("id", "label")

    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")

    withClue("StringIndexer should throw error when setHandleInvalid=error " +
      "when given NULL values") {
      indexer.setHandleInvalid("error")
      testTransformerByInterceptingException[(Int, String)](
        df2,
        indexer.fit(df),
        "StringIndexer encountered NULL value.",
        "labelIndex")
    }

    indexer.setHandleInvalid("skip")
    val modelSkip = indexer.fit(df)
    // a -> 1, b -> 0
    val expectedSkip = Seq((0, 1.0), (1, 0.0)).toDF()
    testTransformerByGlobalCheckFunc[(Int, String)](df2, modelSkip, "id", "labelIndex") { rows =>
      val attrSkip =
        Attribute.fromStructField(rows.head.schema("labelIndex")).asInstanceOf[NominalAttribute]
      assert(attrSkip.values.get === Array("b", "a"))
      assert(rows === expectedSkip.collect().toSeq)
    }

    indexer.setHandleInvalid("keep")
    // a -> 1, b -> 0, null -> 2
    val expectedKeep = Seq((0, 1.0), (1, 0.0), (3, 2.0)).toDF()
    val modelKeep = indexer.fit(df)
    testTransformerByGlobalCheckFunc[(Int, String)](df2, modelKeep, "id", "labelIndex") { rows =>
      val attrKeep = Attribute
        .fromStructField(rows.head.schema("labelIndex"))
        .asInstanceOf[NominalAttribute]
      assert(attrKeep.values.get === Array("b", "a", "__unknown"))
      assert(rows === expectedKeep.collect().toSeq)
    }
  }

  test("StringIndexerModel should keep silent if the input column does not exist.") {
    val indexerModel = new StringIndexerModel("indexer", Array(Array("a", "b", "c")))
      .setInputCol("label")
      .setOutputCol("labelIndex")
    val df = spark.range(0L, 10L).toDF()
    testTransformerByGlobalCheckFunc[Long](df, indexerModel, "id") { rows =>
      assert(rows.toSet === df.collect().toSet)
    }
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

    testTransformerByInterceptingException[(Int, String)](
      df,
      indexer.setOutputCol("output"),
      "Output column output already exists.",
      "labelIndex")

  }

  test("StringIndexer read/write") {
    val t = new StringIndexer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setHandleInvalid("skip")
    testDefaultReadWrite(t)
  }

  test("StringIndexerModel read/write") {
    val instance = new StringIndexerModel("myStringIndexerModel", Array(Array("a", "b", "c")))
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setHandleInvalid("skip")
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.labelsArray(0) === instance.labelsArray(0))
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

    testTransformer[(Int, String)](df0, idxToStr0, "actual", "expected") {
      case Row(actual, expected) =>
        assert(actual === expected)
    }

    val attr = NominalAttribute.defaultAttr.withValues(labels)
    val df1 = df0.select(col("index").as("indexWithAttr", attr.toMetadata()), col("expected"))

    val idxToStr1 = new IndexToString()
      .setInputCol("indexWithAttr")
      .setOutputCol("actual")

    testTransformer[(Int, String)](df1, idxToStr1, "actual", "expected") {
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
      .setLabels(indexer.labelsArray(0))

    testTransformer[(Int, String, Double)](transformed, idx2str, "sameLabel", "label") {
      case Row(sameLabel, label) =>
        assert(sameLabel === label)
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

  test("SPARK 18698: construct IndexToString with custom uid") {
    val uid = "customUID"
    val t = new IndexToString(uid)
    assert(t.uid == uid)
  }

  test("StringIndexer metadata") {
    val data = Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")
      .fit(df)
    testTransformerByGlobalCheckFunc[(Int, String)](df, indexer, "labelIndex") { rows =>
      val attrs =
        NominalAttribute.decodeStructField(rows.head.schema("labelIndex"), preserveName = true)
      assert(attrs.name.nonEmpty && attrs.name.get === "labelIndex")
    }
  }

  test("StringIndexer order types") {
    val data = Seq((0, "b"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "b"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")

    val expected = Seq(Seq((0, 0.0), (1, 0.0), (2, 2.0), (3, 1.0), (4, 1.0), (5, 0.0)),
      Seq((0, 2.0), (1, 2.0), (2, 0.0), (3, 1.0), (4, 1.0), (5, 2.0)),
      Seq((0, 1.0), (1, 1.0), (2, 0.0), (3, 2.0), (4, 2.0), (5, 1.0)),
      Seq((0, 1.0), (1, 1.0), (2, 2.0), (3, 0.0), (4, 0.0), (5, 1.0)))

    var idx = 0
    for (orderType <- StringIndexer.supportedStringOrderType) {
      val model = indexer.setStringOrderType(orderType).fit(df)
      testTransformerByGlobalCheckFunc[(Int, String)](df, model, "id", "labelIndex") { rows =>
        assert(rows === expected(idx).toDF().collect().toSeq)
      }
      idx += 1
    }
  }

  test("StringIndexer order types: secondary sort by alphabets when frequency equal") {
    val data = Seq((0, "a"), (1, "a"), (2, "b"), (3, "b"), (4, "c"), (5, "d"))
    val df = data.toDF("id", "label")
    val indexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("labelIndex")

    val expected = Seq(Set((0, 0.0), (1, 0.0), (2, 1.0), (3, 1.0), (4, 2.0), (5, 3.0)),
      Set((0, 2.0), (1, 2.0), (2, 3.0), (3, 3.0), (4, 0.0), (5, 1.0)))

    var idx = 0
    for (orderType <- Seq("frequencyDesc", "frequencyAsc")) {
      val transformed = indexer.setStringOrderType(orderType).fit(df).transform(df)
      val output = transformed.select("id", "labelIndex").rdd.map { r =>
        (r.getInt(0), r.getDouble(1))
      }.collect().toSet
      assert(output === expected(idx))
      idx += 1
    }
  }

  test("SPARK-22446: StringIndexerModel's indexer UDF should not apply on filtered data") {
    val df = List(
      ("A", "London", "StrA"),
      ("B", "Bristol", null),
      ("C", "New York", "StrC")).toDF("ID", "CITY", "CONTENT")

    val dfNoBristol = df.filter($"CONTENT".isNotNull)

    val model = new StringIndexer()
      .setInputCol("CITY")
      .setOutputCol("CITYIndexed")
      .fit(dfNoBristol)

    testTransformerByGlobalCheckFunc[(String, String, String)](
      dfNoBristol,
      model,
      "CITYIndexed") { rows =>
      assert(rows.toList.count(_.getDouble(0) == 1.0) === 1)
    }
  }

  test("StringIndexer multiple input columns") {
    val data = Seq(
      Row("a", 0.0, "e", 1.0),
      Row("b", 2.0, "f", 0.0),
      Row("c", 1.0, "e", 1.0),
      Row("a", 0.0, "f", 0.0),
      Row("a", 0.0, "f", 0.0),
      Row("c", 1.0, "f", 0.0))

    val schema = StructType(Array(
      StructField("label1", StringType),
      StructField("expected1", DoubleType),
      StructField("label2", StringType),
      StructField("expected2", DoubleType)))

    val df = spark.createDataFrame(sc.parallelize(data), schema)

    val indexer = new StringIndexer()
      .setInputCols(Array("label1", "label2"))
      .setOutputCols(Array("labelIndex1", "labelIndex2"))
    val indexerModel = indexer.fit(df)

    MLTestingUtils.checkCopyAndUids(indexer, indexerModel)

    val transformed = indexerModel.transform(df)

    // Checks output attribute correctness.
    val attr1 = Attribute.fromStructField(transformed.schema("labelIndex1"))
      .asInstanceOf[NominalAttribute]
    assert(attr1.values.get === Array("a", "c", "b"))
    val attr2 = Attribute.fromStructField(transformed.schema("labelIndex2"))
      .asInstanceOf[NominalAttribute]
    assert(attr2.values.get === Array("f", "e"))

    transformed.select("labelIndex1", "expected1").rdd.map { r =>
      (r.getDouble(0), r.getDouble(1))
    }.collect().foreach { case (index, expected) =>
      assert(index == expected)
    }

    transformed.select("labelIndex2", "expected2").rdd.map { r =>
      (r.getDouble(0), r.getDouble(1))
    }.collect().foreach { case (index, expected) =>
      assert(index == expected)
    }
  }

  test("Correctly skipping NULL and NaN values") {
    val df = Seq(("a", Double.NaN), (null, 1.0), ("b", 2.0), (null, 3.0)).toDF("str", "double")

    val indexer = new StringIndexer()
      .setInputCols(Array("str", "double"))
      .setOutputCols(Array("strIndex", "doubleIndex"))

    val model = indexer.fit(df)
    assert(model.labelsArray(0) === Array("a", "b"))
    assert(model.labelsArray(1) === Array("1.0", "2.0", "3.0"))
  }

  test("Load StringIndexderModel prior to Spark 3.0") {
    val modelPath = testFile("ml-models/strIndexerModel-2.4.4")

    val loadedModel = StringIndexerModel.load(modelPath)
    assert(loadedModel.labelsArray === Array(Array("b", "c", "a")))

    val metadata = spark.read.json(s"$modelPath/metadata")
    val sparkVersionStr = metadata.select("sparkVersion").first().getString(0)
    assert(sparkVersionStr === "2.4.4")
  }
}
