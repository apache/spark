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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{IntegerType, StructType}

case class EmptyGenerator() extends Generator {
  override def children: Seq[Expression] = Nil
  override def elementSchema: StructType = new StructType().add("id", IntegerType)
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = Seq.empty
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val iteratorClass = classOf[Iterator[_]].getName
    ev.copy(code = s"$iteratorClass<InternalRow> ${ev.value} = $iteratorClass$$.MODULE$$.empty();")
  }
}

class GeneratorSuite extends QueryTest with SharedSQLContext {
  test("SPARK-14986: Outer lateral view with empty generate expression") {
    checkAnswer(
      sql("select nil from values 1 lateral view outer explode(array()) n as nil"),
      Row(null) :: Nil
    )
  }

  test("outer explode()") {
    checkAnswer(
      sql("select * from values 1, 2 lateral view outer explode(array()) a as b"),
      Row(1, null) :: Row(2, null) :: Nil)
  }

  test("outer generator()") {
    spark.sessionState.functionRegistry.registerFunction("empty_gen", _ => EmptyGenerator())
    checkAnswer(
      sql("select * from values 1, 2 lateral view outer empty_gen() a as b"),
      Row(1, null) :: Row(2, null) :: Nil)
  }
}
