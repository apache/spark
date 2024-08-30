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

import java.util.Iterator;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.LocalScan;
import org.apache.spark.sql.connector.read.Scan;

/**
 * A procedure that is bound to input types.
 *
 * @since 4.0.0
 */
@Evolving
public interface BoundProcedure extends Procedure {
  /**
   * Returns parameters of this procedure.
   */
  ProcedureParameter[] parameters();

  /**
   * Indicates whether this procedure is deterministic.
   */
  boolean isDeterministic();

  /**
   * Executes this procedure with the given input.
   * <p>
   * Spark validates and rearranges arguments provided in the CALL statement to ensure that
   * the order and data types of the fields in {@code input} matches the expected order and
   * types defined by {@link #parameters() parameters}.
   * <p>
   * Each procedure can return any number of result sets. Each result set is represented by
   * a {@link Scan scan} that reports the type of records it produces and can be used to
   * collect the output, if needed. If a result set is local and does not a distributed job,
   * implementations should use {@link LocalScan}.
   */
  Iterator<Scan> call(InternalRow input);
}
