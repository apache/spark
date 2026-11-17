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

package test.org.apache.spark.sql.connector.catalog.functions;

import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * A `bucket` whose bound function implements neither the magic method nor `produceResult`, so
 * Spark finds nothing to call when it resolves the function and cannot evaluate it. It is written
 * in Java because a Scala class gets a forwarder to the default `produceResult`, which Spark finds.
 */
public class JavaBucketNoImpl implements UnboundFunction {
  @Override
  public BoundFunction bind(StructType inputType) {
    return JavaBucketNoImplFunction.INSTANCE;
  }

  @Override
  public String description() {
    return name();
  }

  @Override
  public String name() {
    return "bucket";
  }

  public static class JavaBucketNoImplFunction implements ScalarFunction<Integer> {
    static final JavaBucketNoImplFunction INSTANCE = new JavaBucketNoImplFunction();

    @Override
    public DataType[] inputTypes() {
      return new DataType[] { DataTypes.IntegerType, DataTypes.LongType };
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }

    @Override
    public String name() {
      return "bucket";
    }

    @Override
    public String canonicalName() {
      return name();
    }

    @Override
    public String toString() {
      return name();
    }
  }
}
