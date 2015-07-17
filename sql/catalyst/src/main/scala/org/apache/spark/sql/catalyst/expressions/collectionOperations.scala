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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.types.{IntegerType, MapType, ArrayType, DataType}

/**
 * Given an array or map, returns its size.
 */
case class Size(child: Expression) extends UnaryExpression {

  override def dataType: DataType = IntegerType

  override def checkInputDataTypes(): TypeCheckResult = {
    if (child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"input to function size should be an array or map type, not ${child.dataType}"
      )
    }
  }

  override def eval(input: InternalRow): Int = {
    child.dataType match {
      case ArrayType(_, _) => child.eval(input).asInstanceOf[Seq[Any]].size
      case MapType(_, _, _) => child.eval(input).asInstanceOf[Map[Any, Any]].size
    }
  }
}
