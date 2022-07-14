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

package org.apache.spark.sql.connector.catalog

import org.mockito.Mockito.{mock, when}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructType

class CatalogV2UtilSuite extends SparkFunSuite {
  test("Load relation should encode the identifiers for V2Relations") {
    val testCatalog = mock(classOf[TableCatalog])
    val ident = mock(classOf[Identifier])
    val table = mock(classOf[Table])
    when(table.schema()).thenReturn(new StructType().add("i", "int"))
    when(testCatalog.loadTable(ident)).thenReturn(table)
    val r = CatalogV2Util.loadRelation(testCatalog, ident)
    assert(r.isDefined)
    assert(r.get.isInstanceOf[DataSourceV2Relation])
    val v2Relation = r.get.asInstanceOf[DataSourceV2Relation]
    assert(v2Relation.catalog.exists(_ == testCatalog))
    assert(v2Relation.identifier.exists(_ == ident))
  }
}
