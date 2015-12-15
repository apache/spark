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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

object TestRelations {
  val testRelation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())

  val testRelation2 = LocalRelation(
    AttributeReference("a", StringType)(),
    AttributeReference("b", StringType)(),
    AttributeReference("c", DoubleType)(),
    AttributeReference("d", DecimalType(10, 2))(),
    AttributeReference("e", ShortType)())

  val nestedRelation = LocalRelation(
    AttributeReference("top", StructType(
      StructField("duplicateField", StringType) ::
        StructField("duplicateField", StringType) ::
        StructField("differentCase", StringType) ::
        StructField("differentcase", StringType) :: Nil
    ))())

  val nestedRelation2 = LocalRelation(
    AttributeReference("top", StructType(
      StructField("aField", StringType) ::
        StructField("bField", StringType) ::
        StructField("cField", StringType) :: Nil
    ))())

  val listRelation = LocalRelation(
    AttributeReference("list", ArrayType(IntegerType))())
}
