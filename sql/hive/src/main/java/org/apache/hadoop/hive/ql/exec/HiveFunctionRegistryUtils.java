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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.*;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.internal.SparkLogger;
import org.apache.spark.internal.SparkLoggerFactory;

/**
 * Copy some methods from {@link org.apache.hadoop.hive.ql.exec.FunctionRegistry}
 * to avoid initializing Hive built-in UDFs.
 * <p>
 * The code is based on Hive 2.3.10.
 */
public class HiveFunctionRegistryUtils {

  public static final SparkLogger LOG =
    SparkLoggerFactory.getLogger(HiveFunctionRegistryUtils.class);

  /**
   * This method is shared between UDFRegistry and UDAFRegistry. methodName will
   * be "evaluate" for UDFRegistry, and "aggregate"/"evaluate"/"evaluatePartial"
   * for UDAFRegistry.
   * @throws UDFArgumentException
   */
  public static <T> Method getMethodInternal(Class<? extends T> udfClass,
      String methodName, boolean exact, List<TypeInfo> argumentClasses)
      throws UDFArgumentException {

    List<Method> mlist = new ArrayList<>();

    for (Method m : udfClass.getMethods()) {
      if (m.getName().equals(methodName)) {
        mlist.add(m);
      }
    }

    return getMethodInternal(udfClass, mlist, exact, argumentClasses);
  }

  /**
   * Gets the closest matching method corresponding to the argument list from a
   * list of methods.
   *
   * @param mlist
   *          The list of methods to inspect.
   * @param exact
   *          Boolean to indicate whether this is an exact match or not.
   * @param argumentsPassed
   *          The classes for the argument.
   * @return The matching method.
   */
  public static Method getMethodInternal(Class<?> udfClass, List<Method> mlist, boolean exact,
      List<TypeInfo> argumentsPassed) throws UDFArgumentException {

    // result
    List<Method> udfMethods = new ArrayList<>();
    // The cost of the result
    int leastConversionCost = Integer.MAX_VALUE;

    for (Method m : mlist) {
      List<TypeInfo> argumentsAccepted = TypeInfoUtils.getParameterTypeInfos(m,
          argumentsPassed.size());
      if (argumentsAccepted == null) {
        // null means the method does not accept number of arguments passed.
        continue;
      }

      boolean match = (argumentsAccepted.size() == argumentsPassed.size());
      int conversionCost = 0;

      for (int i = 0; i < argumentsPassed.size() && match; i++) {
        int cost = matchCost(argumentsPassed.get(i), argumentsAccepted.get(i), exact);
        if (cost == -1) {
          match = false;
        } else {
          conversionCost += cost;
        }
      }

      LOG.debug("Method {} match: passed = {} accepted = {} method = {}",
          match ? "did" : "didn't", argumentsPassed, argumentsAccepted, m);

      if (match) {
        // Always choose the function with least implicit conversions.
        if (conversionCost < leastConversionCost) {
          udfMethods.clear();
          udfMethods.add(m);
          leastConversionCost = conversionCost;
          // Found an exact match
          if (leastConversionCost == 0) {
            break;
          }
        } else if (conversionCost == leastConversionCost) {
          // Ambiguous call: two methods with the same number of implicit
          // conversions
          udfMethods.add(m);
          // Don't break! We might find a better match later.
        } else {
          // do nothing if implicitConversions > leastImplicitConversions
        }
      }
    }

    if (udfMethods.size() == 0) {
      // No matching methods found
      throw new NoMatchingMethodException(udfClass, argumentsPassed, mlist);
    }

    if (udfMethods.size() > 1) {
      // First try selecting methods based on the type affinity of the arguments passed
      // to the candidate method arguments.
      filterMethodsByTypeAffinity(udfMethods, argumentsPassed);
    }

    if (udfMethods.size() > 1) {

      // if the only difference is numeric types, pick the method
      // with the smallest overall numeric type.
      int lowestNumericType = Integer.MAX_VALUE;
      boolean multiple = true;
      Method candidate = null;
      List<TypeInfo> referenceArguments = null;

      for (Method m: udfMethods) {
        int maxNumericType = 0;

        List<TypeInfo> argumentsAccepted =
            TypeInfoUtils.getParameterTypeInfos(m, argumentsPassed.size());

        if (referenceArguments == null) {
          // keep the arguments for reference - we want all the non-numeric
          // arguments to be the same
          referenceArguments = argumentsAccepted;
        }

        Iterator<TypeInfo> referenceIterator = referenceArguments.iterator();

        for (TypeInfo accepted: argumentsAccepted) {
          TypeInfo reference = referenceIterator.next();

          boolean acceptedIsPrimitive = false;
          PrimitiveCategory acceptedPrimCat = PrimitiveCategory.UNKNOWN;
          if (accepted.getCategory() == Category.PRIMITIVE) {
            acceptedIsPrimitive = true;
            acceptedPrimCat = ((PrimitiveTypeInfo) accepted).getPrimitiveCategory();
          }
          if (acceptedIsPrimitive && TypeInfoUtils.numericTypes.containsKey(acceptedPrimCat)) {
            // We're looking for the udf with the smallest maximum numeric type.
            int typeValue = TypeInfoUtils.numericTypes.get(acceptedPrimCat);
            maxNumericType = typeValue > maxNumericType ? typeValue : maxNumericType;
          } else if (!accepted.equals(reference)) {
            // There are non-numeric arguments that don't match from one UDF to
            // another. We give up at this point.
            throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
          }
        }

        if (lowestNumericType > maxNumericType) {
          multiple = false;
          lowestNumericType = maxNumericType;
          candidate = m;
        } else if (maxNumericType == lowestNumericType) {
          // multiple udfs with the same max type. Unless we find a lower one
          // we'll give up.
          multiple = true;
        }
      }

      if (!multiple) {
        return candidate;
      } else {
        throw new AmbiguousMethodException(udfClass, argumentsPassed, mlist);
      }
    }
    return udfMethods.get(0);
  }

  public static Object invoke(Method m, Object thisObject, Object... arguments)
      throws HiveException {
    Object o;
    try {
      o = m.invoke(thisObject, arguments);
    } catch (Exception e) {
      StringBuilder argumentString = new StringBuilder();
      if (arguments == null) {
        argumentString.append("null");
      } else {
        argumentString.append("{");
        for (int i = 0; i < arguments.length; i++) {
          if (i > 0) {
            argumentString.append(",");
          }

          argumentString.append(arguments[i]);
        }
        argumentString.append("}");
      }

      String detailedMsg = e instanceof java.lang.reflect.InvocationTargetException ?
        e.getCause().getMessage() : e.getMessage();

      throw new HiveException("Unable to execute method " + m + " with arguments "
          + argumentString + ":" + detailedMsg, e);
    }
    return o;
  }

  /**
   * Returns -1 if passed does not match accepted. Otherwise return the cost
   * (usually 0 for no conversion and 1 for conversion).
   */
  public static int matchCost(TypeInfo argumentPassed,
      TypeInfo argumentAccepted, boolean exact) {
    if (argumentAccepted.equals(argumentPassed)
        || TypeInfoUtils.doPrimitiveCategoriesMatch(argumentPassed, argumentAccepted)) {
      // matches
      return 0;
    }
    if (argumentPassed.equals(TypeInfoFactory.voidTypeInfo)) {
      // passing null matches everything
      return 0;
    }
    if (argumentPassed.getCategory().equals(Category.LIST)
        && argumentAccepted.getCategory().equals(Category.LIST)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedElement = ((ListTypeInfo) argumentPassed)
          .getListElementTypeInfo();
      TypeInfo argumentAcceptedElement = ((ListTypeInfo) argumentAccepted)
          .getListElementTypeInfo();
      return matchCost(argumentPassedElement, argumentAcceptedElement, exact);
    }
    if (argumentPassed.getCategory().equals(Category.MAP)
        && argumentAccepted.getCategory().equals(Category.MAP)) {
      // lists are compatible if and only-if the elements are compatible
      TypeInfo argumentPassedKey = ((MapTypeInfo) argumentPassed)
          .getMapKeyTypeInfo();
      TypeInfo argumentAcceptedKey = ((MapTypeInfo) argumentAccepted)
          .getMapKeyTypeInfo();
      TypeInfo argumentPassedValue = ((MapTypeInfo) argumentPassed)
          .getMapValueTypeInfo();
      TypeInfo argumentAcceptedValue = ((MapTypeInfo) argumentAccepted)
          .getMapValueTypeInfo();
      int cost1 = matchCost(argumentPassedKey, argumentAcceptedKey, exact);
      int cost2 = matchCost(argumentPassedValue, argumentAcceptedValue, exact);
      if (cost1 < 0 || cost2 < 0) {
        return -1;
      }
      return Math.max(cost1, cost2);
    }

    if (argumentAccepted.equals(TypeInfoFactory.unknownTypeInfo)) {
      // accepting Object means accepting everything,
      // but there is a conversion cost.
      return 1;
    }
    if (!exact && TypeInfoUtils.implicitConvertible(argumentPassed, argumentAccepted)) {
      return 1;
    }

    return -1;
  }

  /**
   * Given a set of candidate methods and list of argument types, try to
   * select the best candidate based on how close the passed argument types are
   * to the candidate argument types.
   * For a varchar argument, we would prefer evaluate(string) over evaluate(double).
   * @param udfMethods  list of candidate methods
   * @param argumentsPassed list of argument types to match to the candidate methods
   */
  static void filterMethodsByTypeAffinity(List<Method> udfMethods, List<TypeInfo> argumentsPassed) {
    if (udfMethods.size() > 1) {
      // Prefer methods with a closer signature based on the primitive grouping of each argument.
      // Score each method based on its similarity to the passed argument types.
      int currentScore = 0;
      int bestMatchScore = 0;
      Method bestMatch = null;
      for (Method m: udfMethods) {
        currentScore = 0;
        List<TypeInfo> argumentsAccepted =
            TypeInfoUtils.getParameterTypeInfos(m, argumentsPassed.size());
        Iterator<TypeInfo> argsPassedIter = argumentsPassed.iterator();
        for (TypeInfo acceptedType : argumentsAccepted) {
          // Check the affinity of the argument passed in with the accepted argument,
          // based on the PrimitiveGrouping
          TypeInfo passedType = argsPassedIter.next();
          if (acceptedType.getCategory() == Category.PRIMITIVE
              && passedType.getCategory() == Category.PRIMITIVE) {
            PrimitiveGrouping acceptedPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveTypeInfo) acceptedType).getPrimitiveCategory());
            PrimitiveGrouping passedPg = PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                ((PrimitiveTypeInfo) passedType).getPrimitiveCategory());
            if (acceptedPg == passedPg) {
              // The passed argument matches somewhat closely with an accepted argument
              ++currentScore;
            }
          }
        }
        // Check if the score for this method is any better relative to others
        if (currentScore > bestMatchScore) {
          bestMatchScore = currentScore;
          bestMatch = m;
        } else if (currentScore == bestMatchScore) {
          bestMatch = null; // no longer a best match if more than one.
        }
      }

      if (bestMatch != null) {
        // Found a best match during this processing, use it.
        udfMethods.clear();
        udfMethods.add(bestMatch);
      }
    }
  }
}
