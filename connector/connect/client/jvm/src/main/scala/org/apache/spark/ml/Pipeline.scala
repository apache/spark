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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.sql.types.StructType

/**
 * A stage in a pipeline, either an [[Estimator]] or a [[Transformer]].
 */
abstract class PipelineStage extends Params with Logging {

  /**
   * Check transform validity and derive the output schema from the input schema.
   *
   * We check validity for interactions between parameters during `transformSchema` and raise an
   * exception if any parameter value is invalid. Parameter value checks which do not depend on
   * other parameters are handled by `Param.validate()`.
   *
   * Typical implementation should first conduct verification on schema change and parameter
   * validity, including complex parameter interaction checks.
   */
  def transformSchema(schema: StructType): StructType

  /**
   * :: DeveloperApi ::
   *
   * Derives the output schema from the input schema and parameters, optionally with logging.
   *
   * This should be optimistic. If it is unclear whether the schema will be valid, then it should
   * be assumed valid until proven otherwise.
   */
  @DeveloperApi
  protected def transformSchema(schema: StructType, logging: Boolean): StructType = {
    if (logging) {
      logDebug(s"Input schema: ${schema.json}")
    }
    val outputSchema = transformSchema(schema)
    if (logging) {
      logDebug(s"Expected output schema: ${outputSchema.json}")
    }
    outputSchema
  }

  override def copy(extra: ParamMap): PipelineStage
}
