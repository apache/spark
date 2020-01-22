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

package org.apache.spark.sql.connector;

import org.apache.spark.annotation.Unstable;

import java.util.Map;

/**
 * @since 3.0.0
 */
@Unstable
public interface ExternalCommandRunnerProvider {
  /**
   * Execute an arbitrary string command inside an external execution engine rather than Spark.
   * This could be useful when user wants to execute some commands out of Spark. For
   * example, executing custom DDL/DML command for JDBC, creating index for ElasticSearch,
   * creating cores for Solr and so on.
   *
   * @param command the command provide by user
   * @param parameters data source-specific parameters
   * @return output information from the command
   */
  String[] executeCommand(String command, Map<String, String> parameters);
}
