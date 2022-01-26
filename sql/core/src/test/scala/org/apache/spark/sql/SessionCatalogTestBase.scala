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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.connector.{FakeV2Provider, InMemoryTableSessionCatalog, TestV2SessionCatalogBase}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Table}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SharedSparkSession


trait SessionCatalogTestBase[T <: Table, Catalog <: TestV2SessionCatalogBase[T]]
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfter {

  protected def catalog(name: String): CatalogPlugin =
    spark.sessionState.catalogManager.catalog(name)
  protected val v2Format: String = classOf[FakeV2Provider].getName
  protected val catalogClassName: String = classOf[InMemoryTableSessionCatalog].getName

  before {
    spark.conf.set(V2_SESSION_CATALOG_IMPLEMENTATION.key, catalogClassName)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    catalog(SESSION_CATALOG_NAME).asInstanceOf[Catalog].clearTables()
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
  }
}
