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

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{CATEGORICAL_FEATURES, MAX_CATEGORIES}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class VectorIndexerSuite extends MLTest with DefaultReadWriteTest with Logging {

  import testImplicits._
  import VectorIndexerSuite.FeatureData

  // identical, of length 3
  @transient var densePoints1: DataFrame = _
  @transient var sparsePoints1: DataFrame = _
  @transient var densePoints1TestInvalid: DataFrame = _
  @transient var sparsePoints1TestInvalid: DataFrame = _
  @transient var point1maxes: Array[Double] = _

  // identical, of length 2
  @transient var densePoints2: DataFrame = _
  @transient var sparsePoints2: DataFrame = _

  // different lengths
  @transient var badPoints: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val densePoints1Seq = Seq(
      Vectors.dense(1.0, 2.0, 0.0),
      Vectors.dense(0.0, 1.0, 2.0),
      Vectors.dense(0.0, 0.0, -1.0),
      Vectors.dense(1.0, 3.0, 2.0))
    val densePoints1SeqTestInvalid = densePoints1Seq ++ Seq(
      Vectors.dense(10.0, 2.0, 0.0),
      Vectors.dense(0.0, 10.0, 2.0),
      Vectors.dense(1.0, 3.0, 10.0))
    val sparsePoints1Seq = Seq(
      Vectors.sparse(3, Array(0, 1), Array(1.0, 2.0)),
      Vectors.sparse(3, Array(1, 2), Array(1.0, 2.0)),
      Vectors.sparse(3, Array(2), Array(-1.0)),
      Vectors.sparse(3, Array(0, 1, 2), Array(1.0, 3.0, 2.0)))
    val sparsePoints1SeqTestInvalid = sparsePoints1Seq ++ Seq(
      Vectors.sparse(3, Array(0, 1), Array(10.0, 2.0)),
      Vectors.sparse(3, Array(1, 2), Array(10.0, 2.0)),
      Vectors.sparse(3, Array(0, 1, 2), Array(1.0, 3.0, 10.0)))
    point1maxes = Array(1.0, 3.0, 2.0)

    val densePoints2Seq = Seq(
      Vectors.dense(1.0, 1.0, 0.0, 1.0),
      Vectors.dense(0.0, 1.0, 1.0, 1.0),
      Vectors.dense(-1.0, 1.0, 2.0, 0.0))
    val sparsePoints2Seq = Seq(
      Vectors.sparse(4, Array(0, 1, 3), Array(1.0, 1.0, 1.0)),
      Vectors.sparse(4, Array(1, 2, 3), Array(1.0, 1.0, 1.0)),
      Vectors.sparse(4, Array(0, 1, 2), Array(-1.0, 1.0, 2.0)))

    val badPointsSeq = Seq(
      Vectors.sparse(2, Array(0, 1), Array(1.0, 1.0)),
      Vectors.sparse(3, Array(2), Array(-1.0)))

    // Sanity checks for assumptions made in tests
    assert(densePoints1Seq.head.size == sparsePoints1Seq.head.size)
    assert(densePoints2Seq.head.size == sparsePoints2Seq.head.size)
    assert(densePoints1Seq.head.size != densePoints2Seq.head.size)
    def checkPair(dvSeq: Seq[Vector], svSeq: Seq[Vector]): Unit = {
      assert(dvSeq.zip(svSeq).forall { case (dv, sv) => dv.toArray === sv.toArray },
        "typo in unit test")
    }
    checkPair(densePoints1Seq, sparsePoints1Seq)
    checkPair(densePoints2Seq, sparsePoints2Seq)

    densePoints1 = densePoints1Seq.map(FeatureData).toDF()
    sparsePoints1 = sparsePoints1Seq.map(FeatureData).toDF()
    densePoints1TestInvalid = densePoints1SeqTestInvalid.map(FeatureData).toDF()
    sparsePoints1TestInvalid = sparsePoints1SeqTestInvalid.map(FeatureData).toDF()
    densePoints2 = densePoints2Seq.map(FeatureData).toDF()
    sparsePoints2 = sparsePoints2Seq.map(FeatureData).toDF()
    badPoints = badPointsSeq.map(FeatureData).toDF()
  }

  private def getIndexer: VectorIndexer =
    new VectorIndexer().setInputCol("features").setOutputCol("indexed")

  test("params") {
    ParamsSuite.checkParams(new VectorIndexer)
    val model = new VectorIndexerModel("indexer", 1, Map.empty)
    ParamsSuite.checkParams(model)
  }

  test("Cannot fit an empty DataFrame") {
    val rdd = Array.empty[Vector].map(FeatureData).toSeq.toDF()
    val vectorIndexer = getIndexer
    intercept[NoSuchElementException] {
      vectorIndexer.fit(rdd)
    }
  }

  test("Throws error when given RDDs with different size vectors") {
    val vectorIndexer = getIndexer
    val model = vectorIndexer.fit(densePoints1) // vectors of length 3

    MLTestingUtils.checkCopyAndUids(vectorIndexer, model)

    testTransformer[FeatureData](densePoints1, model, "indexed") { _ => }
    testTransformer[FeatureData](sparsePoints1, model, "indexed") { _ => }

    // If the data is local Dataset, it throws AssertionError directly.
    withClue("Did not throw error when fit, transform were called on " +
      "vectors of different lengths") {
      testTransformerByInterceptingException[FeatureData](
        densePoints2,
        model,
        "VectorIndexerModel expected vector of length 3 but found length 4",
        "indexed")
    }
    // If the data is distributed Dataset, it throws SparkException
    // which is the wrapper of AssertionError.
    withClue("Did not throw error when fit, transform were called " +
      "on vectors of different lengths") {
      testTransformerByInterceptingException[FeatureData](
        densePoints2.repartition(2),
        model,
        "VectorIndexerModel expected vector of length 3 but found length 4",
        "indexed")
    }
    intercept[SparkException] {
      vectorIndexer.fit(badPoints)
      logInfo("Did not throw error when fitting vectors of different lengths in same RDD.")
    }
  }

  test("Same result with dense and sparse vectors") {
    def testDenseSparse(densePoints: DataFrame, sparsePoints: DataFrame): Unit = {
      val denseVectorIndexer = getIndexer.setMaxCategories(2)
      val sparseVectorIndexer = getIndexer.setMaxCategories(2)
      val denseModel = denseVectorIndexer.fit(densePoints)
      val sparseModel = sparseVectorIndexer.fit(sparsePoints)
      val denseMap = denseModel.categoryMaps
      val sparseMap = sparseModel.categoryMaps
      assert(denseMap.keys.toSet == sparseMap.keys.toSet,
        "Categorical features chosen from dense vs. sparse vectors did not match.")
      assert(denseMap == sparseMap,
        "Categorical feature value indexes chosen from dense vs. sparse vectors did not match.")
    }
    testDenseSparse(densePoints1, sparsePoints1)
    testDenseSparse(densePoints2, sparsePoints2)
  }

  test("Builds valid categorical feature value index, transform correctly, check metadata") {
    def checkCategoryMaps(
        data: DataFrame,
        maxCategories: Int,
        categoricalFeatures: Set[Int]): Unit = {
      val collectedData = data.collect().map(_.getAs[Vector](0))

      val errMsg = log"checkCategoryMaps failed for input with " +
        log"maxCategories=${MDC(MAX_CATEGORIES, maxCategories)} " +
        log"categoricalFeatures=${MDC(CATEGORICAL_FEATURES, categoricalFeatures.mkString(", "))}"
      try {
        val vectorIndexer = getIndexer.setMaxCategories(maxCategories)
        val model = vectorIndexer.fit(data)
        val categoryMaps = model.categoryMaps
        // Chose correct categorical features
        assert(categoryMaps.keys.toSet === categoricalFeatures)
        testTransformerByGlobalCheckFunc[FeatureData](data, model, "indexed") { rows =>
          val transformed = rows.map { r => Tuple1(r.getAs[Vector](0)) }.toDF("indexed")
          val indexedRDD: RDD[Vector] = transformed.rdd.map(_.getAs[Vector](0))
          val featureAttrs = AttributeGroup.fromStructField(rows.head.schema("indexed"))
          assert(featureAttrs.name === "indexed")
          assert(featureAttrs.attributes.get.length === model.numFeatures)
          categoricalFeatures.foreach { feature: Int =>
            val origValueSet = collectedData.map(_(feature)).toSet
            val targetValueIndexSet = Range(0, origValueSet.size).toSet
            val catMap = categoryMaps(feature)
            assert(catMap.keys.toSet === origValueSet) // Correct categories
            assert(catMap.values.toSet === targetValueIndexSet) // Correct category indices
            if (origValueSet.contains(0.0)) {
              assert(catMap(0.0) === 0) // value 0 gets index 0
            }
            // Check transformed data
            assert(indexedRDD.map(_(feature)).collect().toSet === targetValueIndexSet)
            // Check metadata
            val featureAttr = featureAttrs(feature)
            assert(featureAttr.index.get === feature)
            featureAttr match {
              case attr: BinaryAttribute =>
                assert(attr.values.get === origValueSet.toArray.sorted.map(_.toString))
              case attr: NominalAttribute =>
                assert(attr.values.get === origValueSet.toArray.sorted.map(_.toString))
                assert(attr.isOrdinal.get === false)
              case _ =>
                throw new RuntimeException(errMsg.message + s". Categorical feature $feature " +
                  s"failed metadata check. Found feature attribute: $featureAttr.")
            }
          }
          // Check numerical feature metadata.
          Range(0, model.numFeatures).filter(feature => !categoricalFeatures.contains(feature))
            .foreach { feature: Int =>
            val featureAttr = featureAttrs(feature)
            featureAttr match {
              case attr: NumericAttribute =>
                assert(featureAttr.index.get === feature)
              case _ =>
                throw new RuntimeException(errMsg.message + s". Numerical feature $feature " +
                  s"failed metadata check. Found feature attribute: $featureAttr.")
            }
          }
        }
      } catch {
        case e: org.scalatest.exceptions.TestFailedException =>
          logError(errMsg)
          throw e
      }
    }
    checkCategoryMaps(densePoints1, maxCategories = 2, categoricalFeatures = Set(0))
    checkCategoryMaps(densePoints1, maxCategories = 3, categoricalFeatures = Set(0, 2))
    checkCategoryMaps(densePoints2, maxCategories = 2, categoricalFeatures = Set(1, 3))
  }

  test("handle invalid") {
    for ((points, pointsTestInvalid) <- Seq((densePoints1, densePoints1TestInvalid),
      (sparsePoints1, sparsePoints1TestInvalid))) {
      val vectorIndexer = getIndexer.setMaxCategories(4).setHandleInvalid("error")
      val model = vectorIndexer.fit(points)
      testTransformerByInterceptingException[FeatureData](
        pointsTestInvalid,
        model,
        "VectorIndexer encountered invalid value",
        "indexed")
      val vectorIndexer1 = getIndexer.setMaxCategories(4).setHandleInvalid("skip")
      val model1 = vectorIndexer1.fit(points)
      val expected = Seq(
        Vectors.dense(1.0, 2.0, 0.0),
        Vectors.dense(0.0, 1.0, 2.0),
        Vectors.dense(0.0, 0.0, 1.0),
        Vectors.dense(1.0, 3.0, 2.0))
      testTransformerByGlobalCheckFunc[FeatureData](pointsTestInvalid, model1, "indexed") { rows =>
        assert(rows.map(_(0)) == expected)
      }
      testTransformerByGlobalCheckFunc[FeatureData](points, model1, "indexed") { rows =>
        assert(rows.map(_(0)) == expected)
      }
      val vectorIndexer2 = getIndexer.setMaxCategories(4).setHandleInvalid("keep")
      val model2 = vectorIndexer2.fit(points)
      testTransformerByGlobalCheckFunc[FeatureData](pointsTestInvalid, model2, "indexed") { rows =>
        assert(rows.map(_(0)) == expected ++ Array(
          Vectors.dense(2.0, 2.0, 0.0),
          Vectors dense(0.0, 4.0, 2.0),
          Vectors.dense(1.0, 3.0, 3.0)))
      }
    }
  }

  test("Maintain sparsity for sparse vectors") {
    def checkSparsity(data: DataFrame, maxCategories: Int): Unit = {
      val points = data.collect().map(_.getAs[Vector](0))
      val vectorIndexer = getIndexer.setMaxCategories(maxCategories)
      val model = vectorIndexer.fit(data)
      testTransformerByGlobalCheckFunc[FeatureData](data, model, "indexed") { rows =>
        points.zip(rows.map(_(0))).foreach {
          case (orig: SparseVector, indexed: SparseVector) =>
            assert(orig.indices.length == indexed.indices.length)
          case _ =>
            // should never happen
            fail("Unit test has a bug in it.")
        }
      }
    }
    checkSparsity(sparsePoints1, maxCategories = 2)
    checkSparsity(sparsePoints2, maxCategories = 2)
  }

  test("Preserve metadata") {
    // For continuous features, preserve name and stats.
    val featureAttributes: Array[Attribute] = point1maxes.zipWithIndex.map { case (maxVal, i) =>
      NumericAttribute.defaultAttr.withName(i.toString).withMax(maxVal)
    }
    val attrGroup = new AttributeGroup("features", featureAttributes)
    val densePoints1WithMeta =
      densePoints1.select(densePoints1("features").as("features", attrGroup.toMetadata()))
    val vectorIndexer = getIndexer.setMaxCategories(2)
    val model = vectorIndexer.fit(densePoints1WithMeta)
    // Check that ML metadata are preserved.
    testTransformerByGlobalCheckFunc[FeatureData](densePoints1WithMeta, model, "indexed") { rows =>
      val transAttributes: Array[Attribute] =
        AttributeGroup.fromStructField(rows.head.schema("indexed")).attributes.get
      featureAttributes.zip(transAttributes).foreach { case (orig, trans) =>
        assert(orig.name === trans.name)
        (orig, trans) match {
          case (orig: NumericAttribute, trans: NumericAttribute) =>
            assert(orig.max.nonEmpty && orig.max === trans.max)
          case _ =>
          // do nothing
          // TODO: Once input features marked as categorical are handled correctly, check that here.
        }
      }
    }
  }

  test("VectorIndexer read/write") {
    val t = new VectorIndexer()
      .setInputCol("myInputCol")
      .setOutputCol("myOutputCol")
      .setMaxCategories(30)
    testDefaultReadWrite(t)
  }

  test("VectorIndexerModel read/write") {
    val categoryMaps = Map(0 -> Map(0.0 -> 0, 1.0 -> 1), 1 -> Map(0.0 -> 0, 1.0 -> 1,
      2.0 -> 2, 3.0 -> 3), 2 -> Map(0.0 -> 0, -1.0 -> 1, 2.0 -> 2))
    val instance = new VectorIndexerModel("myVectorIndexerModel", 3, categoryMaps)
    val newInstance = testDefaultReadWrite(instance)
    assert(newInstance.numFeatures === instance.numFeatures)
    assert(newInstance.categoryMaps === instance.categoryMaps)
  }
}

private[feature] object VectorIndexerSuite {
  case class FeatureData(features: Vector) {
    def getFeatures: Vector = features
  }
}
