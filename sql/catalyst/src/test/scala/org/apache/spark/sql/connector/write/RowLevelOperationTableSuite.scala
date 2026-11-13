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

package org.apache.spark.sql.connector.write

import java.util

import org.mockito.Mockito.mock

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.connector.catalog.{SupportsRowLevelOperations, Table, TableCapability, WriteDistributionMode}
import org.apache.spark.sql.connector.expressions.{FieldReference, NullOrdering, SortDirection, SortOrder}
import org.apache.spark.sql.connector.expressions.LogicalExpressions.sort

class RowLevelOperationTableSuite extends SparkFunSuite {

  test("the write distribution and ordering of the wrapped table are forwarded") {
    val declaredOrdering: Array[SortOrder] = Array(
      sort(FieldReference("id"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST))
    val wrapped = new Table with SupportsRowLevelOperations {
      override def name(): String = "t"
      override def capabilities(): util.Set[TableCapability] = util.Set.of()
      override def writeDistributionMode(): WriteDistributionMode = WriteDistributionMode.RANGE
      override def writeOrdering(): Array[SortOrder] = declaredOrdering
      override def newRowLevelOperationBuilder(
          info: RowLevelOperationInfo): RowLevelOperationBuilder = {
        throw new UnsupportedOperationException()
      }
    }

    val table = RowLevelOperationTable(wrapped, mock(classOf[RowLevelOperation]))

    assert(table.writeDistributionMode() === WriteDistributionMode.RANGE)
    assert(table.writeOrdering().toSeq === declaredOrdering.toSeq)
  }
}
