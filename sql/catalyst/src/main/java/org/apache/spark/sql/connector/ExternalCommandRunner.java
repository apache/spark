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
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * An interface to execute an arbitrary string command inside an external execution engine rather
 * than Spark. This could be useful when user wants to execute some commands out of Spark. For
 * example, executing custom DDL/DML command for JDBC, creating index for ElasticSearch, creating
 * cores for Solr and so on.
 * <p>
 * This interface will be instantiated when end users call `SparkSession#executeCommand`.
 *
 * @since 3.0.0
 */
@Unstable
public interface ExternalCommandRunner {

  /**
   * Execute the given command.
   *
   * @param command The command string provided by users.
   * @param options The user-specified case-insensitive options.
   *
   * @return The output of the command.
   */
  String[] executeCommand(String command, CaseInsensitiveStringMap options);
}
