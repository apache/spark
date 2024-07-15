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

package org.apache.spark.sql.errors

import org.apache.spark.SparkException

/**
 * Object for grouping error messages thrown during parsing/interpreting phase
 * of the SQL Scripting Language interpreter.
 */
private[sql] object SqlScriptingErrors extends QueryErrorsBase {

  def labelsMismatch(beginLabel: String, endLabel: String): Throwable = {
    new SparkException(
      errorClass = "LABELS_MISMATCH",
      cause = null,
      messageParameters = Map("beginLabel" -> beginLabel, "endLabel" -> endLabel))
  }

  def endLabelWithoutBeginLabel(endLabel: String): Throwable = {
    new SparkException(
      errorClass = "END_LABEL_WITHOUT_BEGIN_LABEL",
      cause = null,
      messageParameters = Map("endLabel" -> endLabel))
  }

}
