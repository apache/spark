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

import java.util.concurrent.ThreadLocalRandom

import com.fasterxml.uuid.{Generators, NoArgGenerator}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class RandomBasedUuid() extends LeafExpression with Stateful {

  @transient private var generator: NoArgGenerator = _

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    generator = Generators.randomBasedGenerator(ThreadLocalRandom.current())
  }

  override protected def evalInternal(input: InternalRow): Any = {
    val uuid = generator
      .generate()
      .toString
    UTF8String.fromString(uuid)
  }

  override def nullable: Boolean = false

  override def dataType: DataType = StringType

  override def freshCopy(): RandomBasedUuid = RandomBasedUuid()

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val gen = ctx.freshName("gen")
    ctx.addMutableState("com.fasterxml.uuid.NoArgGenerator",
      gen,
      forceInline = true,
      useFreshName = false)
    ctx.addPartitionInitializationStatement(s"$gen = " +
      "com.fasterxml.uuid.Generators.randomBasedGenerator(" +
      "java.util.concurrent.ThreadLocalRandom.current()" +
      ");")
    ev.copy(code = s"final UTF8String ${ev.value} = " +
      s"UTF8String.fromString(${gen}.generate().toString());",
      isNull = FalseLiteral)
  }
}
