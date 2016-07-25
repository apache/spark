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

package org.apache.spark.sql.execution

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.types._

case class SchemaMappingRelation(
    val dataSchema: StructType,
    val partitionSchema: StructType,
    val catalogSchema: StructType) extends SchemaMapping

class SchemaMappingSuite extends SparkFunSuite {

  val dataSchema = StructType(
     StructField("_col1", IntegerType) ::
     StructField("_col2", LongType) ::
     StructField("_col3", BooleanType) :: Nil)

  val partitionSchema = StructType(
    StructField("part", IntegerType) :: Nil)

  val catalogSchema = StructType(
     StructField("f1", IntegerType) ::
     StructField("f2", LongType) ::
     StructField("f3", BooleanType) ::
     StructField("part", IntegerType) :: Nil)

  val relation = SchemaMappingRelation(dataSchema, partitionSchema, catalogSchema)

  test("looking for data schema field with given catalog field name") {
    val col1 = relation.lookForFieldFromCatalogField("f1").get
    assert(col1.name == "_col1" && col1.dataType == IntegerType)

    val col2 = relation.lookForFieldFromCatalogField("f2").get
    assert(col2.name == "_col2" && col2.dataType == LongType)

    val col3 = relation.lookForFieldFromCatalogField("f3").get
    assert(col3.name == "_col3" && col3.dataType == BooleanType)

    assert(relation.lookForFieldFromCatalogField("f4").isEmpty)
  }

  test("relation with empty catalog schema") {
    val relationWithoutCatalogSchema = SchemaMappingRelation(dataSchema,
      partitionSchema, new StructType())
    assert(relationWithoutCatalogSchema.lookForFieldFromCatalogField("f1").isEmpty)
  }

  test("data schema must match catalog schema in length if catalog schema is not empty") {
    val catalogSchema = StructType(StructField("f1", IntegerType) :: Nil)
    val e = intercept[RuntimeException] {
      SchemaMappingRelation(dataSchema, partitionSchema, catalogSchema)
    }
    assert(e.getMessage.contains("should have the same number of fields"))
  }

  test("transform expression of catalog schema fields to use data schema fields") {
    val attr = AttributeReference("f1", IntegerType)()
    val expr = EqualTo(attr, Literal(1))
    val expected = EqualTo(attr.withName("_col1"), Literal(1))
    assert(relation.transformExpressionToUseDataSchema(expr) == expected)
  }
}
