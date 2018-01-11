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
package org.apache.spark.ml.fpm

import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class FPGrowthSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = FPGrowthSuite.getFPGrowthData(spark)
  }

  test("FPGrowth fit and transform with different data types") {
      class DataTypeWithEncoder[A](val dataType: DataType)
                                  (implicit val encoder: Encoder[(Int, Array[A], Array[A])])

      Array(
        new DataTypeWithEncoder[Int](IntegerType),
        new DataTypeWithEncoder[String](StringType),
        new DataTypeWithEncoder[Short](ShortType),
        new DataTypeWithEncoder[Long](LongType)
        // , new DataTypeWithEncoder[Byte](ByteType)
        // TODO: using ByteType produces error, as Array[Byte] is handled as Binary
        // cannot resolve 'CAST(`items` AS BINARY)' due to data type mismatch:
        // cannot cast array<tinyint> to binary;
      ).foreach { dt => {
        val data = dataset.withColumn("items", col("items").cast(ArrayType(dt.dataType)))
        val model = new FPGrowth().setMinSupport(0.5).fit(data)
        val generatedRules = model.setMinConfidence(0.5).associationRules
        val expectedRules = Seq(
          (Array("2"), Array("1"), 1.0),
          (Array("1"), Array("2"), 0.75)
        ).toDF("antecedent", "consequent", "confidence")
          .withColumn("antecedent", col("antecedent").cast(ArrayType(dt.dataType)))
          .withColumn("consequent", col("consequent").cast(ArrayType(dt.dataType)))
        assert(expectedRules.sort("antecedent").rdd.collect().sameElements(
          generatedRules.sort("antecedent").rdd.collect()))

        val expectedTransformed = Seq(
          (0, Array("1", "2"), Array.emptyIntArray),
          (0, Array("1", "2"), Array.emptyIntArray),
          (0, Array("1", "2"), Array.emptyIntArray),
          (0, Array("1", "3"), Array(2))
        ).toDF("id", "items", "expected")
          .withColumn("items", col("items").cast(ArrayType(dt.dataType)))
          .withColumn("expected", col("expected").cast(ArrayType(dt.dataType)))

        testTransformer(expectedTransformed, model,
          "expected", "prediction") {
          case Row(expected, prediction) => assert(expected === prediction,
            s"Expected $expected but found $prediction for data type $dt")
        }(dt.encoder)
      }
    }
  }

  test("FPGrowth getFreqItems") {
    val model = new FPGrowth().setMinSupport(0.7).fit(dataset)
    val expectedFreq = Seq(
      (Array("1"), 4L),
      (Array("2"), 3L),
      (Array("1", "2"), 3L),
      (Array("2", "1"), 3L) // duplicate as the items sequence is not guaranteed
    ).toDF("items", "expectedFreq")
    val freqItems = model.freqItemsets

    val checkDF = freqItems.join(expectedFreq, "items")
    assert(checkDF.count() == 3 && checkDF.filter(col("freq") === col("expectedFreq")).count() == 3)
  }

  test("FPGrowth getFreqItems with Null") {
    val df = Seq(
      (1, Array("1", "2", "3", "5")),
      (2, Array("1", "2", "3", "4")),
      (3, null.asInstanceOf[Array[String]])
    ).toDF("id", "items")
    val model = new FPGrowth().setMinSupport(0.7).fit(dataset)
    testTransformerByGlobalCheckFunc[(Int, Array[String])](df, model, "id", "prediction") {
      rows => {
        val predictionForId3 = rows.filter(_.getAs[Int]("id") == 3)
          .map(_.getAs[Seq[String]]("prediction"))
        assert(Seq(Seq.empty) === predictionForId3,
          s"Expected empty prediction for id 3, got $predictionForId3")
      }
    }
  }

  test("FPGrowth prediction should not contain duplicates") {
    // This should generate rule 1 -> 3, 2 -> 3
    val dataset = Seq(
      Array("1", "3"),
      Array("2", "3")
    ).toDF("items")
    val model = new FPGrowth().fit(dataset)

    val df = Seq(Array("1", "2")).toDF("items")

    testTransformerByGlobalCheckFunc[(Array[String])](df, model, "prediction") {
      rows => {
        assert(1 === rows.size, s"Expected exactly 1 prediction, got $rows")
        val predictions = rows.map(_.getAs[Seq[String]]("prediction"))
        val expected = Seq(Seq("3"))
        assert(expected === predictions, s"Expected $expected, got $predictions")
      }
    }
  }

  test("FPGrowthModel setMinConfidence should affect rules generation and transform") {
    val model = new FPGrowth().setMinSupport(0.1).setMinConfidence(0.1).fit(dataset)
    val oldRulesNum = model.associationRules.count()
    val oldPredict = model.transform(dataset).withColumnRenamed("prediction", "oldPrediction")

    model.setMinConfidence(0.8765)
    assert(oldRulesNum > model.associationRules.count())
    testTransformer[(Int, Array[String], Array[String])](oldPredict, model,
      "oldPrediction", "prediction") {
      case Row(oldPrediction, prediction) => assert(oldPrediction !== prediction,
          "Change in minConfidence was expected to affect prediction but it remained the same")
    }

    // association rules should stay the same for same minConfidence
    model.setMinConfidence(0.1)
    assert(oldRulesNum === model.associationRules.count())
    testTransformer[(Int, Array[String], Array[String])](oldPredict, model,
      "oldPrediction", "prediction") {
      case Row(oldPrediction, prediction) => assert(oldPrediction === prediction,
        "Changing minConfidence back to original value was expected to produce " +
          s"original predictions. Expected $oldPrediction but found $prediction")
    }
  }

  test("FPGrowth parameter check") {
    val fpGrowth = new FPGrowth().setMinSupport(0.4567)
    val model = fpGrowth.fit(dataset)
      .setMinConfidence(0.5678)
    assert(fpGrowth.getMinSupport === 0.4567)
    assert(model.getMinConfidence === 0.5678)
    // numPartitions should not have default value.
    assert(fpGrowth.isDefined(fpGrowth.numPartitions) === false)
    MLTestingUtils.checkCopyAndUids(fpGrowth, model)
    ParamsSuite.checkParams(fpGrowth)
    ParamsSuite.checkParams(model)
  }

  test("read/write") {
    def checkModelData(model: FPGrowthModel, model2: FPGrowthModel): Unit = {
      assert(model.freqItemsets.collect().toSet.equals(
        model2.freqItemsets.collect().toSet))
      assert(model.associationRules.collect().toSet.equals(
        model2.associationRules.collect().toSet))
      assert(model.setMinConfidence(0.9).associationRules.collect().toSet.equals(
        model2.setMinConfidence(0.9).associationRules.collect().toSet))
    }
    val fPGrowth = new FPGrowth()
    testEstimatorAndModelReadWrite(fPGrowth, dataset, FPGrowthSuite.allParamSettings,
      FPGrowthSuite.allParamSettings, checkModelData)
  }
}

object FPGrowthSuite {

  def getFPGrowthData(spark: SparkSession): DataFrame = {
    spark.createDataFrame(Seq(
      (0, Array("1", "2")),
      (0, Array("1", "2")),
      (0, Array("1", "2")),
      (0, Array("1", "3"))
    )).toDF("id", "items")
  }

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "minSupport" -> 0.321,
    "minConfidence" -> 0.456,
    "numPartitions" -> 5,
    "predictionCol" -> "myPrediction"
  )
}
