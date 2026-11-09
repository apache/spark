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

import java.io.Serializable;

import org.apache.spark.annotation.Evolving;

/**
 * A serializable factory that Spark sends to executors to create {@link ExternalLookupReader}s.
 * Spark creates at most one reader per input partition that a lookup computes, and none if there
 * is nothing to look up, so one task may hold several readers. If the factory, a reader or a
 * cursor throws, the task fails; a failure is never treated as a miss.
 *
 * @since 4.4.0
 */
@Evolving
public interface ExternalLookupReaderFactory extends Serializable {
  /** Creates a reader on the executor; Spark closes it, best-effort, by the end of the task. */
  ExternalLookupReader createReader();
}
