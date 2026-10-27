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

package org.apache.spark.sql.execution.python

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.internal.SQLConf

private[python] final class PythonUDFTiming private (
    evalType: Int,
    val enabled: Boolean) extends Serializable {
  // `enabled` is the plan's saved enable decision; do not reread the timing setting here.
  // Recheck support because the conversion variant can change before execution.
  def workerEnabled(conf: SQLConf): Boolean =
    enabled && PythonUDFTiming.isSupported(evalType, conf)
}

private[python] object PythonUDFTiming {
  def apply(evalType: Int, conf: SQLConf): PythonUDFTiming = {
    new PythonUDFTiming(
      evalType, conf.pythonUDFDetailedTimingEnabled && isSupported(evalType, conf))
  }

  private def isSupported(evalType: Int, conf: SQLConf): Boolean = {
    evalType match {
      case PythonEvalType.SQL_ARROW_BATCHED_UDF => !conf.legacyPandasConversionUDF
      case _ => false
    }
  }
}
