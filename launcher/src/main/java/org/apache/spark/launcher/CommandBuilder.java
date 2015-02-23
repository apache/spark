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

package org.apache.spark.launcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Internal interface that defines a command builder.
 */
interface CommandBuilder {

  /**
   * Builds the command like to execute.
   *
   * @param env A map containing environment variables for the child process. It may already contain
   *            entries defined by the user (such as SPARK_HOME, or those defined by the
   *            SparkLauncher constructor that takes an environment), and may be modified to
   *            include other variables needed by the process to be executed.
   */
  List<String> buildCommand(Map<String, String> env) throws IOException;

}
