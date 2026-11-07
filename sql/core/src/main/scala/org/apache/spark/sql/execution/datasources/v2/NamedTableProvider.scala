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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.connector.catalog.TableProvider

/**
 * A [[TableProvider]] that serves the data sources registered under their own short names, such
 * as Python and native data sources. `DataSource.lookupDataSource` resolves those names to the
 * provider class, so the caller has to tell each new instance which name it was created for with
 * [[setShortName]] before using it.
 */
trait NamedTableProvider extends TableProvider {
  def setShortName(name: String): Unit
}
