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

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Serial;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.DefaultUDAFEvaluatorResolver;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.HiveFunctionRegistryUtils;
import org.apache.hadoop.hive.ql.exec.SparkDefaultUDAFEvaluatorResolver;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluatorResolver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * A equivalent implementation of {@link GenericUDAFBridge}, but eliminate calls
 * of {@link FunctionRegistry} to avoid initializing Hive built-in UDFs.
 * <p>
 * The code is based on Hive 2.3.10.
 */
@SuppressWarnings("deprecation")
public class SparkGenericUDAFBridge extends GenericUDAFBridge {

  public SparkGenericUDAFBridge(UDAF udaf) {
      super(udaf);
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

    UDAFEvaluatorResolver resolver = udaf.getResolver();
    if (resolver instanceof DefaultUDAFEvaluatorResolver) {
      resolver = new SparkDefaultUDAFEvaluatorResolver((DefaultUDAFEvaluatorResolver) resolver);
    }
    Class<? extends UDAFEvaluator> udafEvaluatorClass =
        resolver.getEvaluatorClass(Arrays.asList(parameters));

    return new SparkGenericUDAFBridgeEvaluator(udafEvaluatorClass);
  }

  public static class SparkGenericUDAFBridgeEvaluator
      extends GenericUDAFBridge.GenericUDAFBridgeEvaluator {

    @Serial
    private static final long serialVersionUID = 1L;

    // Used by serialization only
    public SparkGenericUDAFBridgeEvaluator() {
    }

    public SparkGenericUDAFBridgeEvaluator(
        Class<? extends UDAFEvaluator> udafEvaluator) {
      super(udafEvaluator);
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
      HiveFunctionRegistryUtils.invoke(iterateMethod, ((UDAFAgg) agg).ueObject,
          conversionHelper.convertIfNecessary(parameters));
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      HiveFunctionRegistryUtils.invoke(mergeMethod, ((UDAFAgg) agg).ueObject,
          conversionHelper.convertIfNecessary(partial));
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      return HiveFunctionRegistryUtils.invoke(terminateMethod, ((UDAFAgg) agg).ueObject);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      return HiveFunctionRegistryUtils.invoke(terminatePartialMethod, ((UDAFAgg) agg).ueObject);
    }
  }
}
