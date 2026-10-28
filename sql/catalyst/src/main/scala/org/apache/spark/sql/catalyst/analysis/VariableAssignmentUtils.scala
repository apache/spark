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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.VariableReference
import org.apache.spark.sql.catalyst.expressions.Cast.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors.duplicateAssignmentsError

/** Validation for scalar SQL variable assignments. */
object VariableAssignmentUtils {
  /** Rejects duplicate variable identifiers, whose names are normalized at creation. */
  def checkForDuplicateVariables(variables: Seq[VariableReference]): Unit = {
    val duplicates = variables.groupBy(_.identifier).filter(entry => entry._2.length > 1)
    if (duplicates.nonEmpty) {
      throw duplicateAssignmentsError(
        duplicates.keys.map(identifier => toSQLId(identifier.name())).mkString(", "))
    }
  }
}
