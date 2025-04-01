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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.serde2.typeinfo.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * A equivalent implementation of {@link DefaultUDFMethodResolver}, but eliminate calls
 * of {@link org.apache.hadoop.hive.ql.exec.FunctionRegistry} to avoid initializing Hive
 * built-in UDFs.
 * <p>
 * The code is based on Hive 2.3.10.
 */
public class SparkDefaultUDFMethodResolver implements UDFMethodResolver {

  /**
   * The class of the UDF.
   */
  private final Class<? extends UDF> udfClass;

  /**
   * Constructor. This constructor extract udfClass from {@link DefaultUDFMethodResolver}
   */
  @SuppressWarnings("unchecked")
  public SparkDefaultUDFMethodResolver(DefaultUDFMethodResolver wrapped) {
    try {
      Field udfClassField = wrapped.getClass().getDeclaredField("udfClass");
      udfClassField.setAccessible(true);
      this.udfClass = (Class<? extends UDF>) udfClassField.get(wrapped);
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  /**
   * Gets the evaluate method for the UDF given the parameter types.
   *
   * @param argClasses
   *          The list of the argument types that need to matched with the
   *          evaluate function signature.
   */
  @Override
  public Method getEvalMethod(List<TypeInfo> argClasses) throws UDFArgumentException {
    return HiveFunctionRegistryUtils.getMethodInternal(udfClass, "evaluate", false, argClasses);
  }
}
