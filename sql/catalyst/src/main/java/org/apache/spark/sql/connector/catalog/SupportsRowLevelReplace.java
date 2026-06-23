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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.connector.write.RowLevelOperation;

/**
 * A mix-in interface for {@link SupportsRowLevelOperations} that signals a table can handle the
 * {@link RowLevelOperation.Command#REPLACE} command.
 * <p>
 * Spark dispatches {@code INSERT INTO ... REPLACE USING (cols)} through this command.
 * {@link RowLevelOperation.Command#REPLACE} is dispatched to a connector's
 * {@link SupportsRowLevelOperations#newRowLevelOperationBuilder} only when the table implements
 * this interface. Tables that support row-level operations but do not implement this interface
 * will have {@code INSERT INTO ... REPLACE} rejected during analysis, so connectors that only
 * expect {@code DELETE}, {@code UPDATE}, or {@code MERGE} are never asked to build a
 * {@code REPLACE} operation.
 *
 * @since 4.2.0
 */
@Experimental
public interface SupportsRowLevelReplace extends SupportsRowLevelOperations {
}
