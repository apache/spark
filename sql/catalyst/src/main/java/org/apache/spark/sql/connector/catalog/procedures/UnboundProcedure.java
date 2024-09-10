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
 * A procedure that is not bound to input types.
 *
 * @since 4.0.0
 */
@Evolving
public interface UnboundProcedure extends Procedure {
  /**
   * Binds this procedure to input types.
   * <p>
   * If the catalog supports procedure overloading, the implementation is expected to pick the best
   * matching version of the procedure. If overloading is not supported, the implementation can
   * validate if the input types are compatible while binding or delegate that to Spark. Regardless,
   * Spark will always perform the final validation of the arguments and rearrange them as needed
   * based on {@link BoundProcedure#parameters() reported parameters}.
   *
   * @param inputType the input types to bind to
   * @return the bound procedure that is most suitable for the given input types
   */
  BoundProcedure bind(StructType inputType);
}
