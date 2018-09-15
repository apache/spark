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

package org.apache.spark.sql.sources.v2;

import org.apache.spark.annotation.InterfaceStability;

/**
 * The base interface for data source v2. Implementations must have a public, 0-arg constructor.
 *
 * Note that this is an empty interface. Data source implementations must mix in interfaces such as
 * {@link BatchReadSupportProvider} or {@link BatchWriteSupportProvider}, which can provide
 * batch or streaming read/write support instances. Otherwise it's just a dummy data source which
 * is un-readable/writable.
 *
 * If Spark fails to execute any methods in the implementations of this interface (by throwing an
 * exception), the read action will fail and no Spark job will be submitted.
 */
@InterfaceStability.Evolving
public interface DataSourceV2 {}
