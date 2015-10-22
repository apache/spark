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

package org.apache.spark.sql.catalyst.encoders

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateSafeProjection, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}
import org.apache.spark.sql.types.{ObjectType, StructType}

/**
 * A generic encoder for JVM objects.
 *
 * @param schema The schema after converting `T` to a Spark SQL row.
 * @param extractExpressions A set of expressions, one for each top-level field that can be used to
 *                           extract the values from a raw object.
 * @param clsTag A classtag for `T`.
 */
case class ClassEncoder[T](
    schema: StructType,
    extractExpressions: Seq[Expression],
    constructExpression: Expression,
    clsTag: ClassTag[T])
  extends Encoder[T] {

  private val extractProjection = GenerateUnsafeProjection.generate(extractExpressions)
  private val inputRow = new GenericMutableRow(1)

  private lazy val constructProjection = GenerateSafeProjection.generate(constructExpression :: Nil)
  private val dataType = ObjectType(clsTag.runtimeClass)

  override def toRow(t: T): InternalRow = {
    inputRow(0) = t
    extractProjection(inputRow)
  }

  override def fromRow(row: InternalRow): T = {
    constructProjection(row).get(0, dataType).asInstanceOf[T]
  }

  override def bind(schema: Seq[Attribute]): ClassEncoder[T] = {
    val plan = Project(Alias(constructExpression, "object")() :: Nil, LocalRelation(schema))
    val analyzedPlan = SimpleAnalyzer.execute(plan)
    val resolvedExpression = analyzedPlan.expressions.head.children.head
    val boundExpression = BindReferences.bindReference(resolvedExpression, schema)

    copy(constructExpression = boundExpression)
  }
}
