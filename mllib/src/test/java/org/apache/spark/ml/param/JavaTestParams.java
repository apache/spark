package org.apache.spark.ml.param;

import org.apache.spark.ml.param.Params;
import org.apache.spark.ml.param.shared.HasMaxIter;

/**
 * A subclass of Params for testing.
 */
public class JavaTestParams extends JavaParams {

  public IntParam myIntParam;

  public DoubleParam myDoubleParam;

  public int getMyIntParam() { return (Integer)getOrDefault(myIntParam); }

  public double getMyDoubleParam() { return (Double)getOrDefault(myDoubleParam); }

  public JavaTestParams setMyIntParam(int value) {
    set(myIntParam, value); return this;
  }

  public JavaTestParams setMyDoubleParam(double value) {
    set(myDoubleParam, value); return this;
  }

  public JavaTestParams() {
    myIntParam =
      new IntParam(this, "myIntParam", "this is an int param", ParamValidate.gt(0));
    myDoubleParam =
      new DoubleParam(this, "myDoubleParam", "this is a double param",
        ParamValidate.and(ParamValidate.gtEq(0.0), ParamValidate.gt(1.0));
    setDefault(myIntParam.w(1));
  }
}
