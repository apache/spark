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
 * Classes that implement this interface can deep-compare [for equality only,
 * not order] with another instance. They do a deep compare. If there is any
 * semantically significant difference, an implementer throws an Exception to be
 * thrown with a chain of causes describing the chain of field references and
 * indices that get you to the miscompared point.
 * 
 */
public interface DeepCompare {
  /**
   * @param other
   *          the other comparand that's being compared to me
   * @param myLocation
   *          the path that got to me. In the root, myLocation is null. To
   *          process the scalar {@code foo} field of the root we will make a
   *          recursive call with a {@link TreePath} whose {@code fieldName} is
   *          {@code "bar"} and whose {@code index} is -1 and whose {@code
   *          parent} is {@code null}. To process the plural {@code bar} field
   *          of the root we will make a recursive call with a {@link TreePath}
   *          whose fieldName is {@code "foo"} and whose {@code index} is -1 and
   *          whose {@code parent} is also {@code null}.
   * @throws DeepInequalityException
   */
  public void deepCompare(DeepCompare other, TreePath myLocation)
      throws DeepInequalityException;
}
