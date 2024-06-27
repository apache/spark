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
package org.apache.spark.sql.execution.benchmark

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{Bin, Expression, ImplicitCastInputTypes, NullIntolerant, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, LongType}
import org.apache.spark.unsafe.types.UTF8String

object MathFunctionBenchmark extends SqlBasedBenchmark {
  private val N = 200L * 1000 * 1000

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val benchmark = new Benchmark("BIN", N, output = output)
    benchmark.addCase("BIN") { _ =>
      spark.range(N).select(Column(Bin(Column("id").expr))).noop()
    }

    benchmark.addCase("BIN OLD") { _ =>
      spark.range(N).select(Column(BinOld(Column("id").expr))).noop()
    }
    benchmark.run()
  }
}

case class BinOld(child: Expression)
  extends UnaryExpression with ImplicitCastInputTypes with NullIntolerant with Serializable {

  override def inputTypes: Seq[DataType] = Seq(LongType)
  override def dataType: DataType = SQLConf.get.defaultStringType

  protected override def nullSafeEval(input: Any): Any =
    UTF8String.fromString(java.lang.Long.toBinaryString(input.asInstanceOf[Long]))

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    defineCodeGen(ctx, ev, (c) =>
      s"UTF8String.fromString(java.lang.Long.toBinaryString($c))")
  }
  override protected def withNewChildInternal(newChild: Expression): BinOld =
    copy(child = newChild)
}
