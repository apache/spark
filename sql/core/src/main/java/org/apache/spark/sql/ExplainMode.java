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
package org.apache.spark.sql;

import org.apache.spark.annotation.Unstable;

/**
 * ExplainMode is used to specify the expected output format of plans (logical and physical)
 * for debugging purpose.
 *
 * @since 3.0.0
 */
@Unstable
public enum ExplainMode {
  /**
   * Simple mode means that when printing explain for a DataFrame, only a physical plan is
   * expected to be printed to the console.
   *
   * @since 3.0.0
   */
  Simple,
  /**
   * Extended mode means that when printing explain for a DataFrame, both logical and physical
   * plans are expected to be printed to the console.
   *
   * @since 3.0.0
   */
  Extended,
  /**
   * Codegen mode means that when printing explain for a DataFrame, if generated codes are
   * available, a physical plan and the generated codes are expected to be printed to the console.
   *
   * @since 3.0.0
   */
  Codegen,
  /**
   * Cost mode means that when printing explain for a DataFrame, if plan node statistics are
   * available, a logical plan and the statistics are expected to be printed to the console.
   *
   * @since 3.0.0
   */
  Cost,
  /**
   * Formatted mode means that when printing explain for a DataFrame, explain output is
   * expected to be split into two sections: a physical plan outline and node details.
   *
   * @since 3.0.0
   */
  Formatted
}
