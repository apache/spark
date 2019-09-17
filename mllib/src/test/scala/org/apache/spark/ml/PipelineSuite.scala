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

package org.apache.spark.ml

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.Pipeline.SharedReadWrite
import org.apache.spark.ml.feature.{HashingTF, MinMaxScaler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.{IntParam, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType

class PipelineSuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  abstract class MyModel extends Model[MyModel]

  test("pipeline") {
    val estimator0 = mock[Estimator[MyModel]]
    val model0 = mock[MyModel]
    val transformer1 = mock[Transformer]
    val estimator2 = mock[Estimator[MyModel]]
    val model2 = mock[MyModel]
    val transformer3 = mock[Transformer]
    val dataset0 = mock[DataFrame]
    val dataset1 = mock[DataFrame]
    val dataset2 = mock[DataFrame]
    val dataset3 = mock[DataFrame]
    val dataset4 = mock[DataFrame]

    when(dataset0.toDF).thenReturn(dataset0)
    when(dataset1.toDF).thenReturn(dataset1)
    when(dataset2.toDF).thenReturn(dataset2)
    when(dataset3.toDF).thenReturn(dataset3)
    when(dataset4.toDF).thenReturn(dataset4)

    when(estimator0.copy(any[ParamMap])).thenReturn(estimator0)
    when(model0.copy(any[ParamMap])).thenReturn(model0)
    when(transformer1.copy(any[ParamMap])).thenReturn(transformer1)
    when(estimator2.copy(any[ParamMap])).thenReturn(estimator2)
    when(model2.copy(any[ParamMap])).thenReturn(model2)
    when(transformer3.copy(any[ParamMap])).thenReturn(transformer3)

    when(estimator0.fit(meq(dataset0))).thenReturn(model0)
    when(model0.transform(meq(dataset0))).thenReturn(dataset1)
    when(model0.parent).thenReturn(estimator0)
    when(transformer1.transform(meq(dataset1))).thenReturn(dataset2)
    when(estimator2.fit(meq(dataset2))).thenReturn(model2)
    when(model2.transform(meq(dataset2))).thenReturn(dataset3)
    when(model2.parent).thenReturn(estimator2)
    when(transformer3.transform(meq(dataset3))).thenReturn(dataset4)

    val pipeline = new Pipeline()
      .setStages(Array(estimator0, transformer1, estimator2, transformer3))
    val pipelineModel = pipeline.fit(dataset0)

    MLTestingUtils.checkCopyAndUids(pipeline, pipelineModel)

    assert(pipelineModel.stages.length === 4)
    assert(pipelineModel.stages(0).eq(model0))
    assert(pipelineModel.stages(1).eq(transformer1))
    assert(pipelineModel.stages(2).eq(model2))
    assert(pipelineModel.stages(3).eq(transformer3))

    val output = pipelineModel.transform(dataset0)
    assert(output.eq(dataset4))
  }

  test("pipeline with duplicate stages") {
    val estimator = mock[Estimator[MyModel]]
    val pipeline = new Pipeline()
      .setStages(Array(estimator, estimator))
    val dataset = mock[DataFrame]
    intercept[IllegalArgumentException] {
      pipeline.fit(dataset)
    }
  }

  test("Pipeline.copy") {
    val hashingTF = new HashingTF()
      .setNumFeatures(100)
    val pipeline = new Pipeline("pipeline").setStages(Array[Transformer](hashingTF))
    val copied = pipeline.copy(ParamMap(hashingTF.numFeatures -> 10))

    assert(copied.uid === pipeline.uid,
      "copy should create an instance with the same UID")
    assert(copied.getStages(0).asInstanceOf[HashingTF].getNumFeatures === 10,
      "copy should handle extra stage params")
  }

  test("PipelineModel.copy") {
    val hashingTF = new HashingTF()
      .setNumFeatures(100)
    val model = new PipelineModel("pipelineModel", Array[Transformer](hashingTF))
      .setParent(new Pipeline())
    val copied = model.copy(ParamMap(hashingTF.numFeatures -> 10))

    assert(copied.uid === model.uid,
      "copy should create an instance with the same UID")
    assert(copied.stages(0).asInstanceOf[HashingTF].getNumFeatures === 10,
      "copy should handle extra stage params")
    assert(copied.parent === model.parent,
      "copy should create an instance with the same parent")
  }

  test("pipeline model constructors") {
    val transform0 = mock[Transformer]
    val model1 = mock[MyModel]

    val stages = Array(transform0, model1)
    val pipelineModel0 = new PipelineModel("pipeline0", stages)
    assert(pipelineModel0.uid === "pipeline0")
    assert(pipelineModel0.stages === stages)

    val stagesAsList = stages.toList.asJava
    val pipelineModel1 = new PipelineModel("pipeline1", stagesAsList)
    assert(pipelineModel1.uid === "pipeline1")
    assert(pipelineModel1.stages === stages)
  }

  test("Pipeline read/write") {
    val writableStage = new WritableStage("writableStage").setIntParam(56)
    val pipeline = new Pipeline().setStages(Array(writableStage))

    val pipeline2 = testDefaultReadWrite(pipeline, testParams = false)
    assert(pipeline2.getStages.length === 1)
    assert(pipeline2.getStages(0).isInstanceOf[WritableStage])
    val writableStage2 = pipeline2.getStages(0).asInstanceOf[WritableStage]
    assert(writableStage.getIntParam === writableStage2.getIntParam)
  }

  test("Pipeline read/write with non-Writable stage") {
    val unWritableStage = new UnWritableStage("unwritableStage")
    val unWritablePipeline = new Pipeline().setStages(Array(unWritableStage))
    withClue("Pipeline.write should fail when Pipeline contains non-Writable stage") {
      intercept[UnsupportedOperationException] {
        unWritablePipeline.write
      }
    }
  }

  test("PipelineModel read/write") {
    val writableStage = new WritableStage("writableStage").setIntParam(56)
    val pipeline =
      new PipelineModel("pipeline_89329327", Array(writableStage.asInstanceOf[Transformer]))

    val pipeline2 = testDefaultReadWrite(pipeline, testParams = false)
    assert(pipeline2.stages.length === 1)
    assert(pipeline2.stages(0).isInstanceOf[WritableStage])
    val writableStage2 = pipeline2.stages(0).asInstanceOf[WritableStage]
    assert(writableStage.getIntParam === writableStage2.getIntParam)
  }

  test("PipelineModel read/write: getStagePath") {
    val stageUid = "myStage"
    val stagesDir = new Path("pipeline", "stages").toString
    def testStage(stageIdx: Int, numStages: Int, expectedPrefix: String): Unit = {
      val path = SharedReadWrite.getStagePath(stageUid, stageIdx, numStages, stagesDir)
      val expected = new Path(stagesDir, expectedPrefix + "_" + stageUid).toString
      assert(path === expected)
    }
    testStage(0, 1, "0")
    testStage(0, 9, "0")
    testStage(0, 10, "00")
    testStage(1, 10, "01")
    testStage(12, 999, "012")
  }

  test("PipelineModel read/write with non-Writable stage") {
    val unWritableStage = new UnWritableStage("unwritableStage")
    val unWritablePipeline =
      new PipelineModel("pipeline_328957", Array(unWritableStage.asInstanceOf[Transformer]))
    withClue("PipelineModel.write should fail when PipelineModel contains non-Writable stage") {
      intercept[UnsupportedOperationException] {
        unWritablePipeline.write
      }
    }
  }

  test("pipeline validateParams") {
    val df = Seq(
      (1, Vectors.dense(0.0, 1.0, 4.0), 1.0),
      (2, Vectors.dense(1.0, 0.0, 4.0), 2.0),
      (3, Vectors.dense(1.0, 0.0, 5.0), 3.0),
      (4, Vectors.dense(0.0, 0.0, 5.0), 4.0)
    ).toDF("id", "features", "label")

    intercept[IllegalArgumentException] {
       val scaler = new MinMaxScaler()
         .setInputCol("features")
         .setOutputCol("features_scaled")
         .setMin(10)
         .setMax(0)
       val pipeline = new Pipeline().setStages(Array(scaler))
       pipeline.fit(df)
    }
  }

  test("Pipeline.setStages should handle Java Arrays being non-covariant") {
    val stages0 = Array(new UnWritableStage("b"))
    val stages1 = Array(new WritableStage("a"))
    val steps = stages0 ++ stages1
    val p = new Pipeline().setStages(steps)
  }
}


/**
 * Used to test [[Pipeline]] with `MLWritable` stages
 */
class WritableStage(override val uid: String) extends Transformer with MLWritable {

  final val intParam: IntParam = new IntParam(this, "intParam", "doc")

  def getIntParam: Int = $(intParam)

  def setIntParam(value: Int): this.type = set(intParam, value)

  setDefault(intParam -> 0)

  override def copy(extra: ParamMap): WritableStage = defaultCopy(extra)

  override def write: MLWriter = new DefaultParamsWriter(this)

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF

  override def transformSchema(schema: StructType): StructType = schema
}

object WritableStage extends MLReadable[WritableStage] {

  override def read: MLReader[WritableStage] = new DefaultParamsReader[WritableStage]

  override def load(path: String): WritableStage = super.load(path)
}

/**
 * Used to test [[Pipeline]] with non-`MLWritable` stages
 */
class UnWritableStage(override val uid: String) extends Transformer {

  final val intParam: IntParam = new IntParam(this, "intParam", "doc")

  setDefault(intParam -> 0)

  override def copy(extra: ParamMap): UnWritableStage = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF

  override def transformSchema(schema: StructType): StructType = schema
}
