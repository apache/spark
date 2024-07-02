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

package org.apache.spark.sql.connector.catalog.procedures;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.types.StructType;

/**
 * A stored procedure that can be invoked via SQL.
 * <p>
 * This is a special type of the procedure that returns at most one result set and knows
 * the type of output rows ahead of time. This is the only type of the procedure that can
 * be invoked via CALL statements in SQL. Procedures that determine the output type of rows
 * during the procedure execution or return multiple results have to be invoked via the
 * dedicated programmatic API.
 *
 * @since 4.1.0
 */
@Evolving
public interface SQLInvocableProcedure extends BoundProcedure {
  /**
   * Returns the output type of rows produced by this procedure.
   */
  StructType outputType();
}
