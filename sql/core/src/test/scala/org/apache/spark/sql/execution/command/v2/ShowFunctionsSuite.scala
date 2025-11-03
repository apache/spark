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

package org.apache.spark.sql.execution.command.v2

import test.org.apache.spark.sql.connector.catalog.functions.JavaStrLen
import test.org.apache.spark.sql.connector.catalog.functions.JavaStrLen.JavaStrLenNoImpl

import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper
import org.apache.spark.sql.execution.command
import org.apache.spark.util.ArrayImplicits._

/**
 * The class contains tests for the `SHOW FUNCTIONS` command to check V2 table catalogs.
 */
class ShowFunctionsSuite extends command.ShowFunctionsSuiteBase with CommandSuiteBase {
  override protected def funCatalog: String = s"fun_$catalog"

  private def getFunCatalog(): InMemoryCatalog = {
    spark.sessionState.catalogManager.catalog(funCatalog).asInstanceOf[InMemoryCatalog]
  }

  private def funNameToId(name: String): Identifier = {
    val parts = name.split('.')
    assert(parts.head == funCatalog, s"${parts.head} is wrong catalog. Expected: $funCatalog.")
    new MultipartIdentifierHelper(parts.tail.toImmutableArraySeq).asIdentifier
  }

  override protected def createFunction(name: String): Unit = {
    getFunCatalog().createFunction(funNameToId(name), new JavaStrLen(new JavaStrLenNoImpl))
  }

  override protected def dropFunction(name: String): Unit = {
    getFunCatalog().dropFunction(funNameToId(name))
  }
}
