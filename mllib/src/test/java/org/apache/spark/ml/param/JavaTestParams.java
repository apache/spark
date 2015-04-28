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

package org.apache.spark.ml.param;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * A subclass of Params for testing.
 */
public class JavaTestParams extends JavaParams {

  public IntParam myIntParam;

  public int getMyIntParam() { return (Integer)getOrDefault(myIntParam); }

  public JavaTestParams setMyIntParam(int value) {
    set(myIntParam, value); return this;
  }

  public DoubleParam myDoubleParam;

  public double getMyDoubleParam() { return (Double)getOrDefault(myDoubleParam); }

  public JavaTestParams setMyDoubleParam(double value) {
    set(myDoubleParam, value); return this;
  }

  public Param<String> myStringParam;

  public String getMyStringParam() { return (String)getOrDefault(myStringParam); }

  public JavaTestParams setMyStringParam(String value) {
    set(myStringParam, value); return this;
  }

  public JavaTestParams() {
    myIntParam = new IntParam(this, "myIntParam", "this is an int param", ParamValidators.gt(0));
    myDoubleParam = new DoubleParam(this, "myDoubleParam", "this is a double param",
      ParamValidators.inRange(0.0, 1.0));
    List<String> validStrings = Lists.newArrayList("a", "b");
    myStringParam = new Param<String>(this, "myStringParam", "this is a string param",
      ParamValidators.inArray(validStrings));
    setDefault(myIntParam, 1);
    setDefault(myDoubleParam, 0.5);
  }
}
