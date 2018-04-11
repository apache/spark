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

import java.util.Random

import com.fasterxml.uuid.Generators

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, FalseLiteral}

@ExpressionDescription(
  usage = "_FUNC_() - Returns a random-based universally unique identifier (UUID)." +
    " The value is returned as a canonical UUID 36-character string.",
  examples = """
    Examples:
      > SELECT _FUNC_();
       46707d92-02f4-4817-8116-a4c3b23e6266
  """,
  since = "2.4.0"
)
case class RandomBasedUuid(randomSeed: Option[Long] = None) extends UuidExpression {

  def this() = this(None)

  override lazy val resolved: Boolean = randomSeed.isDefined

  override protected def initializeInternal(partitionIndex: Int): Unit = {
    generator = Generators.randomBasedGenerator(new Random(randomSeed.get + partitionIndex))
  }

  override def freshCopy(): RandomBasedUuid = RandomBasedUuid(randomSeed)

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val gen = ctx.freshName("gen")
    ctx.addMutableState("com.fasterxml.uuid.NoArgGenerator",
      gen,
      forceInline = true,
      useFreshName = false)
    ctx.addPartitionInitializationStatement(s"$gen = " +
      "com.fasterxml.uuid.Generators.randomBasedGenerator(" +
      s"new java.util.Random(${randomSeed.get}L + partitionIndex)" +
      ");")
    ev.copy(code = s"final UTF8String ${ev.value} = " +
      s"UTF8String.fromString(${gen}.generate().toString());",
      isNull = FalseLiteral)
  }
}
