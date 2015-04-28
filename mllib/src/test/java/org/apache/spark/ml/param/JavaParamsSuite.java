package org.apache.spark.ml.param;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.api.java.JavaSparkContext;

/**
 * Test Param and related classes in Java
 */
public class JavaParamsSuite {

  private transient JavaSparkContext jsc;

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaParamsSuite");
  }

  @After
  public void tearDown() {
    jsc.stop();
    jsc = null;
  }

  @Test
  public void testParams() {
    JavaTestParams testParams = new JavaTestParams();
    Assert.assertEquals(testParams.getMyIntParam(), 1);
    testParams.setMyIntParam(2).setMyDoubleParam(0.4).setMyStringParam("a");
    Assert.assertEquals(testParams.getMyDoubleParam(), 0.4, 0.0);
    Assert.assertEquals(testParams.getMyStringParam(), "a");
  }

  @Test
  public void testParamValidate() {
    ParamValidate.alwaysTrue();
    ParamValidate.gt(1.0);
    ParamValidate.gtEq(1.0);
    ParamValidate.lt(1.0);
    ParamValidate.ltEq(1.0);
    ParamValidate.inRange(0, 1, true, false);
    ParamValidate.inRange(0, 1);
    ParamValidate.inArray(Lists.newArrayList(0, 1, 3));
    ParamValidate.inArray(Lists.newArrayList("a", "b"));
  }
}
