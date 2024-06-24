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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.execution.command

/**
 * The class contains tests for the `ALTER TABLE .. SET TBLPROPERTIES` command to
 * check V2 table catalogs.
 */
class AlterTableSetTblPropertiesSuite
  extends command.AlterTableSetTblPropertiesSuiteBase with CommandSuiteBase {

  private def normalizeTblProps(props: Map[String, String]): Map[String, String] = {
    props.filterNot(p => Seq("provider", "owner").contains(p._1))
  }

  private def getTableMetadata(tableIndent: TableIdentifier): Table = {
    val nameParts = tableIndent.nameParts
    val v2Catalog = spark.sessionState.catalogManager.catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  override def checkTblProps(tableIdent: TableIdentifier,
      expectedTblProps: Map[String, String]): Unit = {
    val actualTblProps = getTableMetadata(tableIdent).properties.asScala.toMap
    assert(normalizeTblProps(actualTblProps) === expectedTblProps)
  }
}
