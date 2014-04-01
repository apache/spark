/**
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
package org.apache.hadoop.tools.rumen;

/**
 * We use this exception class in the unit test, and we do a deep comparison
 * when we run the
 * 
 */
public class DeepInequalityException extends Exception {

  static final long serialVersionUID = 1352469876;

  final TreePath path;

  /**
   * @param message
   *          an exception message
   * @param path
   *          the path that gets from the root to the inequality
   * 
   *          This is the constructor that I intend to have used for this
   *          exception.
   */
  public DeepInequalityException(String message, TreePath path,
      Throwable chainee) {
    super(message, chainee);

    this.path = path;
  }

  /**
   * @param message
   *          an exception message
   * @param path
   *          the path that gets from the root to the inequality
   * 
   *          This is the constructor that I intend to have used for this
   *          exception.
   */
  public DeepInequalityException(String message, TreePath path) {
    super(message);

    this.path = path;
  }
}
