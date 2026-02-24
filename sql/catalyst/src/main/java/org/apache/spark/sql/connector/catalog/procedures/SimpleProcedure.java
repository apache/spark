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
 * A procedure that does not require binding to input types.
 * <p>
 * This interface is designed for procedures that have no overloads and do not need custom binding
 * logic. Implementations can directly provide procedure parameters and execution logic without
 * implementing the {@link UnboundProcedure#bind(StructType) bind} method.
 * <p>
 * The default {@link #bind(StructType) bind} method simply returns {@code this}, as the procedure
 * is already considered bound.
 *
 * @since 4.2.0
 */
@Evolving
public interface SimpleProcedure extends UnboundProcedure, BoundProcedure {
  @Override
  default BoundProcedure bind(StructType inputType) {
    return this;
  }
}

