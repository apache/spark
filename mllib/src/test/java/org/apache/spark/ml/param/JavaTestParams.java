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

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.util.Identifiable$;
import scala.reflect.ClassTag;

/**
 * A subclass of Params for testing.
 */
public class JavaTestParams extends JavaParams {

  public JavaTestParams() {
    this.uid_ = Identifiable$.MODULE$.randomUID("javaTestParams");
    init();
  }

  public JavaTestParams(String uid) {
    this.uid_ = uid;
    init();
  }

  private String uid_;

  @Override
  public String uid() {
    return uid_;
  }

  private IntParam myIntParam_;

  public IntParam myIntParam() {
    return myIntParam_;
  }

  public int getMyIntParam() {
    return (Integer) getOrDefault(myIntParam_);
  }

  public JavaTestParams setMyIntParam(int value) {
    set(myIntParam_, value);
    return this;
  }

  private DoubleParam myDoubleParam_;

  public DoubleParam myDoubleParam() {
    return myDoubleParam_;
  }

  public double getMyDoubleParam() {
    return (Double) getOrDefault(myDoubleParam_);
  }

  public JavaTestParams setMyDoubleParam(double value) {
    set(myDoubleParam_, value);
    return this;
  }

  private Param<String> myStringParam_;

  public Param<String> myStringParam() {
    return myStringParam_;
  }

  public String getMyStringParam() {
    return getOrDefault(myStringParam_);
  }

  public JavaTestParams setMyStringParam(String value) {
    set(myStringParam_, value);
    return this;
  }

  private DoubleArrayParam myDoubleArrayParam_;

  public DoubleArrayParam myDoubleArrayParam() {
    return myDoubleArrayParam_;
  }

  public double[] getMyDoubleArrayParam() {
    return getOrDefault(myDoubleArrayParam_);
  }

  public JavaTestParams setMyDoubleArrayParam(double[] value) {
    set(myDoubleArrayParam_, value);
    return this;
  }

  private void init() {
    myIntParam_ = new IntParam(this, "myIntParam", "this is an int param", ParamValidators.gt(0));
    myDoubleParam_ = new DoubleParam(this, "myDoubleParam", "this is a double param",
      ParamValidators.inRange(0.0, 1.0));
    List<String> validStrings = Arrays.asList("a", "b");
    myStringParam_ = new Param<>(this, "myStringParam", "this is a string param",
      ParamValidators.inArray(validStrings), ClassTag.apply(String.class));
    myDoubleArrayParam_ =
      new DoubleArrayParam(this, "myDoubleArrayParam", "this is a double param");

    setDefault(myIntParam(), 1);
    setDefault(myDoubleParam(), 0.5);
    setDefault(myDoubleArrayParam(), new double[]{1.0, 2.0});
  }

  @Override
  public JavaTestParams copy(ParamMap extra) {
    return defaultCopy(extra);
  }
}
