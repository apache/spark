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

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

public class GenericUDAFSum2 extends AbstractGenericUDAFResolver {
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }
    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
      case BYTE: case SHORT: case INT: case LONG:
        return new GenericUDAFSumLong();
      case TIMESTAMP: case FLOAT: case DOUBLE: case STRING: case VARCHAR: case CHAR:
        return new GenericUDAFSumDouble();
      case DECIMAL:
        return new GenericUDAFSumHiveDecimal();
      default:
        throw new UDFArgumentTypeException(0,
            "Only numeric or string type arguments are accepted but "
                + parameters[0].getTypeName() + " is passed.");
    }
  }

  public static class GenericUDAFSumLong extends GenericUDAFEvaluator {
    private PrimitiveObjectInspector inputOI;
    private LongWritable result;

    @AggregationType(estimable = true)
    static class SumLongAgg extends AbstractAggregationBuffer {
      boolean empty;
      long sum;

      @Override
      public int estimate() {
        return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2;
      }
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      result = new LongWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumLongAgg r = new SumLongAgg();
      reset(r);
      return r;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumLongAgg a = (SumLongAgg) agg;
      a.empty = true;
      a.sum = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg)
        throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        SumLongAgg a = (SumLongAgg) agg;
        a.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
        a.empty = false;
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumLongAgg a = (SumLongAgg) agg;
      if (a.empty) {
        return null;
      }
      result.set(a.sum);
      return result;
    }
  }

  public static class GenericUDAFSumDouble extends GenericUDAFEvaluator {
    private PrimitiveObjectInspector inputOI;
    private DoubleWritable result;

    @AggregationType(estimable = true)
    static class SumDoubleAgg extends AbstractAggregationBuffer {
      boolean empty;
      double sum;

      @Override
      public int estimate() {
        return JavaDataModel.PRIMITIVES1 + JavaDataModel.PRIMITIVES2;
      }
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      result = new DoubleWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumDoubleAgg r = new SumDoubleAgg();
      reset(r);
      return r;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg a = (SumDoubleAgg) agg;
      a.empty = true;
      a.sum = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg)
        throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        SumDoubleAgg a = (SumDoubleAgg) agg;
        a.empty = false;
        a.sum += PrimitiveObjectInspectorUtils.getDouble(partial, inputOI);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumDoubleAgg a = (SumDoubleAgg) agg;
      if (a.empty) {
        return null;
      }
      result.set(a.sum);
      return result;
    }
  }

  public static class GenericUDAFSumHiveDecimal extends GenericUDAFEvaluator {
    private PrimitiveObjectInspector inputOI;
    private HiveDecimalWritable result;

    static class SumHiveDecimalAgg extends AbstractAggregationBuffer {
      boolean empty;
      HiveDecimal sum;
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      result = new HiveDecimalWritable(HiveDecimal.ZERO);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      DecimalTypeInfo outputTypeInfo = null;
      if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
        int precision = Math.min(
            HiveDecimal.MAX_PRECISION, inputOI.precision() + 10);
        outputTypeInfo = TypeInfoFactory.getDecimalTypeInfo(
            precision, inputOI.scale());
      } else {
        outputTypeInfo = (DecimalTypeInfo) inputOI.getTypeInfo();
      }
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(outputTypeInfo);
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      SumHiveDecimalAgg r = new SumHiveDecimalAgg();
      reset(r);
      return r;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      SumHiveDecimalAgg a = (SumHiveDecimalAgg) agg;
      a.empty = true;
      a.sum = HiveDecimal.ZERO;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg)
        throws HiveException {
      return terminate(agg);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        SumHiveDecimalAgg a = (SumHiveDecimalAgg) agg;
        if (a.sum == null) {
          return;
        }
        a.empty = false;
        a.sum = a.sum.add(
            PrimitiveObjectInspectorUtils.getHiveDecimal(partial, inputOI));
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      SumHiveDecimalAgg a = (SumHiveDecimalAgg) agg;
      if (a.empty || a.sum == null) {
        return null;
      }
      result.set(a.sum);
      return result;
    }
  }
}
