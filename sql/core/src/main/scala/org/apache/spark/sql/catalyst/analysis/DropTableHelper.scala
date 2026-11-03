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

import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan}
import org.apache.spark.sql.execution.command.{DropTableCommand, DropTempViewCommand}
import org.apache.spark.sql.internal.SQLConf

/**
 * Rewrites a resolved [[DropTable]] to its session-catalog command in [[ResolveSessionCatalog]].
 * Other catalogs keep the V2 command.
 */
object DropTableHelper {
  def rewrite(dropTable: DropTable): LogicalPlan = dropTable match {
    case DropTable(ResolvedV1CommandIdentifier(identifier), ifExists, purge)
        if SQLConf.get.useV1Command =>
      DropTableCommand(identifier, ifExists, isView = false, purge = purge)
    case DropTable(ResolvedIdentifier(FakeSystemCatalog, identifier), _, _) =>
      DropTempViewCommand(identifier)
    case _ =>
      dropTable
  }
}
