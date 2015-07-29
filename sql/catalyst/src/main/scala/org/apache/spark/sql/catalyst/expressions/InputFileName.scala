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

import org.apache.spark.rdd.SqlNewHadoopRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GeneratedExpressionCode, CodeGenContext}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

private[sql] case class InputFileName() extends LeafExpression with Nondeterministic {

  override def nullable: Boolean = true

  override def dataType: DataType = StringType

  @transient private[this] var fileName: UTF8String = _

  override protected def initInternal(): Unit = {
    fileName = UTF8String.fromString(SqlNewHadoopRDD.getInputFileName())
  }

  override protected def evalInternal(input: InternalRow): UTF8String = fileName

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val nameTerm = ctx.freshName("fileName")
    ctx.addMutableState(ctx.javaType(StringType), nameTerm,
      s"""$nameTerm = UTF8String.fromString(
         |org.apache.spark.rdd.SqlNewHadoopRDD.getInputFileName());""".stripMargin)
    ev.isNull = "false"
    s"final ${ctx.javaType(dataType)} ${ev.primitive} = $nameTerm;"
  }

}
