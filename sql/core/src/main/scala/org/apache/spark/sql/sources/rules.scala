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

package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Alias}
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.DataType

private[sql] case class PreInsertCastAndRename(
    resolver: (String, String) => Boolean) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case i@InsertIntoTable(
      l @ LogicalRelation(r: InsertableRelation), partition, child, overwrite) => {
        if (l.output.size != child.output.size) {
          sys.error(
            s"$l requires that the query in the SELECT clause of the INSERT INTO/OVERWRITE " +
              s"statement generates the same number of columns as its schema.")
        }
        castAndRenameChildOutput(i, l.output, child, r.ignoreNullability)
      }
    }
  }

  /** If necessary, cast data types and rename fields to the expected types and names. */
  def castAndRenameChildOutput(
      insertInto: InsertIntoTable,
      expectedOutput: Seq[Attribute],
      child: LogicalPlan,
      ignoreNullability: Boolean) = {
    def sameDataType(dataType1: DataType, dataType2: DataType, ignoreNullability: Boolean) = {
      if (ignoreNullability)
        DataType.equalsIgnoreNullability(dataType1, dataType2)
      else
        dataType1 == dataType2
    }

    val newChildOutput = expectedOutput.zip(child.output).map {
      case (expected, actual) =>
        val needCast = !sameDataType(expected.dataType, actual.dataType, ignoreNullability)
        val needRename = !resolver(expected.name, actual.name)
        (needCast, needRename) match {
          case (true, _) => Alias(Cast(actual, expected.dataType), expected.name)()
          case (false, true) => Alias(actual, expected.name)()
          case (_, _) => actual
        }
    }

    if (newChildOutput == child.output)
      insertInto
    else
      insertInto.copy(child = Project(newChildOutput, child))
  }

}