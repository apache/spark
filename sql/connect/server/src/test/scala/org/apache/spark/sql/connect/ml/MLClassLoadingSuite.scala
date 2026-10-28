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

import org.apache.spark.connect.proto
import org.apache.spark.ml.{Model, Transformer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCols, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.connect.{ResourceHelper, SparkConnectTestUtils}
import org.apache.spark.sql.types.StructType

// A harmless ML-readable model deliberately omitted from the ServiceLoader registrations.
class UnregisteredModel(override val uid: String)
    extends Model[UnregisteredModel]
    with HasInputCols
    with HasOutputCol
    with DefaultParamsWritable {

  override def transform(dataset: Dataset[_]): DataFrame = dataset.toDF()

  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): UnregisteredModel = defaultCopy(extra)
}

object UnregisteredModel extends DefaultParamsReadable[UnregisteredModel]

class MLClassLoadingSuite extends MLHelper with ResourceHelper {

  private def withRestoredSessionHolder[T](f: => T): T = {
    val previousSessionHolder = MLHandler.currentSessionHolder.get()
    try {
      f
    } finally {
      if (previousSessionHolder == null) {
        MLHandler.currentSessionHolder.remove()
      } else {
        MLHandler.currentSessionHolder.set(previousSessionHolder)
      }
    }
  }

  private def readModel(fixture: String): proto.MlCommandResult = {
    val path = baseResourcePath.resolve("ml-class-loading").resolve(fixture)
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
          .setPath(path.toAbsolutePath.toString))
      .build()
    withRestoredSessionHolder {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
      MLHandler.handleMlCommand(sessionHolder, command)
    }
  }

  test("load an RFormulaModel with an allowlisted nested stage") {
    val result = readModel("allowed-model")
    assert(result.getOperatorInfo.getUid == "rFormulaFixture")
    assert(result.getOperatorInfo.getObjRef.getId.nonEmpty)
  }

  test("the unregistered nested model is readable outside Spark Connect") {
    val path = baseResourcePath.resolve("ml-class-loading")
      .resolve("rejected-model/pipelineModel/stages/0_stageFixture")
    val model = withRestoredSessionHolder {
      MLHandler.currentSessionHolder.remove()
      UnregisteredModel.load(path.toAbsolutePath.toString)
    }
    assert(model.uid == "stageFixture")
    assert(model.getInputCols.toSeq == Seq("x"))
    assert(model.getOutputCol == "features")
    val transformers = MLUtils.loadOperators(classOf[Transformer])
    assert(!transformers.contains(classOf[UnregisteredModel].getName))
  }

  test("reject an RFormulaModel with a non-allowlisted nested stage") {
    val error = intercept[MlUnsupportedException] {
      readModel("rejected-model")
    }
    assert(error.getMessage.contains(classOf[UnregisteredModel].getName))
    assert(error.getMessage.contains("not in the allowlist"))
  }
}
