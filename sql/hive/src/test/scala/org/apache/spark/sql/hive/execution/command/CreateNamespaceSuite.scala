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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.execution.command.v1

/**
 * The class contains tests for the `CREATE NAMESPACE` command to check V1 Hive external
 * table catalog.
 */
class CreateNamespaceSuite extends v1.CreateNamespaceSuiteBase with CommandSuiteBase {
  override def commandVersion: String = super[CreateNamespaceSuiteBase].commandVersion
  override def alreadyExistErrorMessage: String = s"$notFoundMsgPrefix $namespace already exists"
}
