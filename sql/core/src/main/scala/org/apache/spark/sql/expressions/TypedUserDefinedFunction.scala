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

import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.TypedScalaUDF
import org.apache.spark.sql.Column
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.{lit, struct}

case class TypedUserDefinedFunction[T1: Encoder, R: Encoder](f: T1 => R) {
  def apply(exprs: Column*): Column = {
    val t1Encoder = encoderFor[T1]
    val rEncoder = encoderFor[R]
    val expr1 = exprs match {
      case Seq() => lit(0)
      case Seq(expr1) => expr1
      case exprs => struct(exprs: _*)
    }
    Column(TypedScalaUDF(f, t1Encoder, rEncoder, expr1.expr))
  }
}
