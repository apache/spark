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
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.internal.SQLConf

/**
 * Encapsulates the evaluation context for expressions, capturing SQL configuration
 * state at expression construction time.
 *
 * This context must be stored as part of the expression's state to ensure deterministic
 * evaluation. Without it, copying an expression or evaluating it in a different context
 * (e.g., inside a view) could produce different results due to changed SQL configuration
 * values.
 *
 * @param evalMode                  The error handling mode (LEGACY, ANSI, or TRY) that determines
 *                                  overflow behavior and exception handling for operations like
 *                                  arithmetic and casts.
 * @param allowDecimalPrecisionLoss Whether decimal operations are allowed to lose precision
 *                                  when the result type cannot represent the full precision.
 *                                  Corresponds to
 *                                  spark.sql.decimalOperations.allowPrecisionLoss.
 */
case class NumericEvalContext private(
    evalMode: EvalMode.Value,
    allowDecimalPrecisionLoss: Boolean
)

case object NumericEvalContext {

  def apply(
      evalMode: EvalMode.Value,
      allowDecimalPrecisionLoss: Boolean = SQLConf.get.decimalOperationsAllowPrecisionLoss
  ): NumericEvalContext = {
    new NumericEvalContext(evalMode, allowDecimalPrecisionLoss)
  }

  def fromSQLConf(conf: SQLConf): NumericEvalContext = {
    NumericEvalContext(
      EvalMode.fromSQLConf(conf),
      conf.decimalOperationsAllowPrecisionLoss)
  }
}
