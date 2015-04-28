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
    myIntParam = new IntParam(this, "myIntParam", "this is an int param", ParamValidate.gt(0));
    myDoubleParam = new DoubleParam(this, "myDoubleParam", "this is a double param",
      ParamValidate.inRange(0.0, 1.0));
    List<String> validStrings = Lists.newArrayList("a", "b");
    myStringParam = new Param<String>(this, "myStringParam", "this is a string param",
      ParamValidate.inArray(validStrings));
    setDefault(myIntParam.w(1), myDoubleParam.w(0.5));
  }
}
