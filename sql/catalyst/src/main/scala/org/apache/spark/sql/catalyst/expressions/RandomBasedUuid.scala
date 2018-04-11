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

import com.fasterxml.uuid.Generators

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}

case class RandomBasedUuid() extends UuidExpression {

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    generator = Generators.randomBasedGenerator(ThreadLocalRandom.current())
  }

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
