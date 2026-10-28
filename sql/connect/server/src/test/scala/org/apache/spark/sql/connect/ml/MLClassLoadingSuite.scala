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
import org.apache.spark.sql.connect.{ResourceHelper, SparkConnectTestUtils}

class MLClassLoadingSuite extends MLHelper with ResourceHelper {

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
    val previousSessionHolder = MLHandler.currentSessionHolder.get()
    try {
      val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
      MLHandler.handleMlCommand(sessionHolder, command)
    } finally {
      if (previousSessionHolder == null) {
        MLHandler.currentSessionHolder.remove()
      } else {
        MLHandler.currentSessionHolder.set(previousSessionHolder)
      }
    }
  }

  test("load an RFormulaModel with an allowlisted nested stage") {
    val result = readModel("allowed-model")
    assert(result.getOperatorInfo.getUid == "rFormulaFixture")
    assert(result.getOperatorInfo.getObjRef.getId.nonEmpty)
  }

  test("reject an RFormulaModel with a non-allowlisted nested stage") {
    val error = intercept[MlUnsupportedException] {
      readModel("rejected-model")
    }
    assert(error.getMessage.contains(
      "org.apache.spark.sql.connect.ml.UnregisteredTestStage"))
    assert(error.getMessage.contains("not in the allowlist"))
  }
}
