/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop;

import java.lang.annotation.*;

/**
 * A package attribute that captures the version of Hadoop that was compiled.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PACKAGE)
public @interface HadoopVersionAnnotation {
 
  /**
   * Get the Hadoop version
   * @return the version string "0.6.3-dev"
   */
  String version();
  
  /**
   * Get the username that compiled Hadoop.
   */
  String user();
  
  /**
   * Get the date when Hadoop was compiled.
   * @return the date in unix 'date' format
   */
  String date();
  
  /**
   * Get the url for the subversion repository.
   */
  String url();
  
  /**
   * Get the subversion revision.
   * @return the revision number as a string (eg. "451451")
   */
  String revision();

  /**
   * Get the branch from which this was compiled.
   * @return The branch name, e.g. "trunk" or "branches/branch-0.20"
   */
  String branch();

  /**
   * Get a checksum of the source files from which
   * Hadoop was compiled.
   * @return a string that uniquely identifies the source
   **/
  String srcChecksum();
}
