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

package org.apache.spark.sql.connect.ml

import java.io.File

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.ml.feature.{RFormula, RFormulaModel, RFormulaModelTestHelper, VectorAssembler}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.types.StructType

// A harmless ML-readable model deliberately omitted from the ServiceLoader registrations.
class UnregisteredModel(override val uid: String)
    extends Model[UnregisteredModel]
    with DefaultParamsWritable {

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): UnregisteredModel = defaultCopy(extra)
}

object UnregisteredModel extends DefaultParamsReadable[UnregisteredModel]

class MLClassLoadingSuite extends MLHelper {

  override protected def afterEach(): Unit = {
    try {
      MLHandler.currentSessionHolder.remove()
    } finally {
      super.afterEach()
    }
  }

  private def withSavedModel(stage: Transformer)(f: String => Unit): Unit = {
    withTempDir { dir =>
      val path = new File(dir, "model").getAbsolutePath
      RFormulaModelTestHelper.withStage(stage).write.save(path)
      f(path)
    }
  }

  private def readModel(path: String): RFormulaModel = {
    val command = proto.MlCommand
      .newBuilder()
      .setRead(
        proto.MlCommand.Read
          .newBuilder()
          .setOperator(
            proto.MlOperator
              .newBuilder()
              .setName("org.apache.spark.ml.feature.RFormulaModel")
              .setType(proto.MlOperator.OperatorType.OPERATOR_TYPE_MODEL))
          .setPath(path))
      .build()
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val result = MLHandler.handleMlCommand(sessionHolder, command)
    val modelId = result.getOperatorInfo.getObjRef.getId
    assert(modelId.nonEmpty)
    sessionHolder.mlCache.get(modelId).asInstanceOf[RFormulaModel]
  }

  test("load an RFormulaModel with an allowlisted nested stage") {
    val stage = new VectorAssembler("stageFixture")
      .setInputCols(Array("x"))
      .setOutputCol("features")
    withSavedModel(stage) { path =>
      val model = readModel(path)
      assert(model.uid == "rFormulaFixture")
    }
  }

  test("load a fitted RFormulaModel with internal nested stages") {
    val dataset = spark.createDataFrame(
      Seq((1.0, 1.0, "a"), (0.0, 2.0, "b"), (1.0, 0.0, "a"))).toDF("y", "x", "s")
    val model = new RFormula()
      .setFormula("y ~ x + s")
      .setStringIndexerOrderType("alphabetDesc")
      .fit(dataset)
    val expected = model.transform(dataset).select("features", "label").collect().toSeq

    withTempDir { dir =>
      val path = new File(dir, "model").getAbsolutePath
      model.write.save(path)
      val loadedModel = readModel(path)
      assert(loadedModel.uid == model.uid)
      assert(loadedModel.getFormula == model.getFormula)
      assert(loadedModel.transform(dataset).select("features", "label").collect().toSeq == expected)
    }
  }

  test("the unregistered nested model is readable outside Spark Connect") {
    val stage = new UnregisteredModel("stageFixture")
    withSavedModel(stage) { path =>
      val nestedPath = new File(path, s"pipelineModel/stages/0_${stage.uid}").getAbsolutePath
      MLHandler.currentSessionHolder.remove()
      val model = UnregisteredModel.load(nestedPath)
      assert(model.uid == stage.uid)
      val transformers = MLUtils.loadOperators(classOf[Transformer])
      assert(!transformers.contains(classOf[UnregisteredModel].getName))
    }
  }

  test("reject an RFormulaModel with a non-allowlisted nested stage") {
    withSavedModel(new UnregisteredModel("stageFixture")) { path =>
      val error = intercept[MlUnsupportedException] {
        readModel(path)
      }
      assert(error.getMessage.contains(classOf[UnregisteredModel].getName))
      assert(error.getMessage.contains("not in the allowlist"))
    }
  }
}
