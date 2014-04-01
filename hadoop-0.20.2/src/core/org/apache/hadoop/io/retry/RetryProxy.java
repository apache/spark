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
package org.apache.hadoop.io.retry;

import java.lang.reflect.Proxy;
import java.util.Map;

/**
 * <p>
 * A factory for creating retry proxies.
 * </p>
 */
public class RetryProxy {
  /**
   * <p>
   * Create a proxy for an interface of an implementation class
   * using the same retry policy for each method in the interface. 
   * </p>
   * @param iface the interface that the retry will implement
   * @param implementation the instance whose methods should be retried
   * @param retryPolicy the policy for retirying method call failures
   * @return the retry proxy
   */
  public static Object create(Class<?> iface, Object implementation,
                              RetryPolicy retryPolicy) {
    return Proxy.newProxyInstance(
                                  implementation.getClass().getClassLoader(),
                                  new Class<?>[] { iface },
                                  new RetryInvocationHandler(implementation, retryPolicy)
                                  );
  }  
  
  /**
   * <p>
   * Create a proxy for an interface of an implementation class
   * using the a set of retry policies specified by method name.
   * If no retry policy is defined for a method then a default of
   * {@link RetryPolicies#TRY_ONCE_THEN_FAIL} is used.
   * </p>
   * @param iface the interface that the retry will implement
   * @param implementation the instance whose methods should be retried
   * @param methodNameToPolicyMap a map of method names to retry policies
   * @return the retry proxy
   */
  public static Object create(Class<?> iface, Object implementation,
                              Map<String,RetryPolicy> methodNameToPolicyMap) {
    return Proxy.newProxyInstance(
                                  implementation.getClass().getClassLoader(),
                                  new Class<?>[] { iface },
                                  new RetryInvocationHandler(implementation, methodNameToPolicyMap)
                                  );
  }
}
