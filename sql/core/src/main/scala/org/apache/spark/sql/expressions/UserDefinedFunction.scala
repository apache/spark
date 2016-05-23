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

package org.apache.spark.sql.expressions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.expressions.ScalaUDF
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.DataType

/**
 * A user-defined function. To create one, use the `udf` functions in [[functions]].
 * Note that the user-defined functions must be deterministic. Due to optimization,
 * duplicate invocations may be eliminated or the function may even be invoked more times than
 * it is present in the query.
 * As an example:
 * {{{
 *   // Defined a UDF that returns true or false based on some numeric score.
 *   val predict = udf((score: Double) => if (score > 0.5) true else false)
 *
 *   // Projects a column that adds a prediction column based on the score column.
 *   df.select( predict(df("score")) )
 * }}}
 *
 * @since 1.3.0
 */
@Experimental
case class UserDefinedFunction protected[sql] (
    f: AnyRef,
    dataType: DataType,
    inputTypes: Option[Seq[DataType]]) {

  def apply(exprs: Column*): Column = {
    Column(ScalaUDF(f, dataType, exprs.map(_.expr), inputTypes.getOrElse(Nil)))
  }
}
