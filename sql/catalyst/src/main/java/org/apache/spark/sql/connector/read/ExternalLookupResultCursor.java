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

package org.apache.spark.sql.connector.read;

import java.io.Closeable;
import java.io.IOException;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.InternalRow;

/**
 * A cursor over the results of one {@link ExternalLookupReader#lookupBatch} call. Each row is one
 * match for the parameter row at {@link #inputOrdinal()}: an ordinal may repeat, and an absent
 * ordinal is a miss. If the read schema has no columns, each match is still one (empty) row.
 * <p>
 * {@link #inputOrdinal()} and {@link #get()} are valid after {@link #next()} returns
 * {@code true}, until the next {@code next} or {@link #close()}. The row may be reused; Spark
 * copies the rows it keeps. Spark may close the cursor before its end.
 *
 * @since 4.4.0
 */
@Evolving
public interface ExternalLookupResultCursor extends Closeable {
  /** Advances to the next result, or returns {@code false} at the end. */
  boolean next() throws IOException;

  /** Returns the current result's parameter-row index, in {@code [0, uniqueParameters.length)}. */
  int inputOrdinal();

  /** Returns the current value row, whose schema is {@link Scan#readSchema()}. */
  InternalRow get();
}
