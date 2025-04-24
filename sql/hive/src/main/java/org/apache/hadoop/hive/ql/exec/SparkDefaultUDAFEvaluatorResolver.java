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

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * A equivalent implementation of {@link DefaultUDAFEvaluatorResolver}, but eliminate calls
 * of {@link FunctionRegistry} to avoid initializing Hive built-in UDFs.
 * <p>
 * The code is based on Hive 2.3.10.
 */
@SuppressWarnings("deprecation")
public class SparkDefaultUDAFEvaluatorResolver implements UDAFEvaluatorResolver {

  /**
   * The class of the UDAF.
   */
  private final Class<? extends UDAF> udafClass;

  /**
   * Constructor. This constructor extract udafClass from {@link DefaultUDAFEvaluatorResolver}
   */
  @SuppressWarnings("unchecked")
  public SparkDefaultUDAFEvaluatorResolver(DefaultUDAFEvaluatorResolver wrapped) {
    try {
      Field udfClassField = wrapped.getClass().getDeclaredField("udafClass");
      udfClassField.setAccessible(true);
      this.udafClass = (Class<? extends UDAF>) udfClassField.get(wrapped);
    } catch (ReflectiveOperationException rethrow) {
      throw new RuntimeException(rethrow);
    }
  }

  /**
   * Gets the evaluator class for the UDAF given the parameter types.
   *
   * @param argClasses
   *          The list of the parameter types.
   */
  @SuppressWarnings("unchecked")
  public Class<? extends UDAFEvaluator> getEvaluatorClass(
      List<TypeInfo> argClasses) throws UDFArgumentException {

    ArrayList<Class<? extends UDAFEvaluator>> classList = new ArrayList<>();

    // Add all the public member classes that implement an evaluator
    for (Class<?> enclClass : udafClass.getClasses()) {
      if (UDAFEvaluator.class.isAssignableFrom(enclClass)) {
        classList.add((Class<? extends UDAFEvaluator>) enclClass);
      }
    }

    // Next we locate all the iterate methods for each of these classes.
    ArrayList<Method> mList = new ArrayList<>();
    ArrayList<Class<? extends UDAFEvaluator>> cList = new ArrayList<>();
    for (Class<? extends UDAFEvaluator> evaluator : classList) {
      for (Method m : evaluator.getMethods()) {
        if (m.getName().equalsIgnoreCase("iterate")) {
          mList.add(m);
          cList.add(evaluator);
        }
      }
    }

    Method m = HiveFunctionRegistryUtils.getMethodInternal(udafClass, mList, false, argClasses);

    // Find the class that has this method.
    // Note that Method.getDeclaringClass() may not work here because the method
    // can be inherited from a base class.
    int found = -1;
    for (int i = 0; i < mList.size(); i++) {
      if (mList.get(i) == m) {
        if (found == -1) {
          found = i;
        } else {
          throw new AmbiguousMethodException(udafClass, argClasses, mList);
        }
      }
    }
    assert (found != -1);

    return cList.get(found);
  }
}
