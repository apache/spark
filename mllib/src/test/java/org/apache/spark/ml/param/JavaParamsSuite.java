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

import org.junit.Assert;
import org.junit.Test;

/**
 * Test Param and related classes in Java
 */
public class JavaParamsSuite {

  @Test
  public void testParams() {
    JavaTestParams testParams = new JavaTestParams();
    Assert.assertEquals(1, testParams.getMyIntParam());
    testParams.setMyIntParam(2).setMyDoubleParam(0.4).setMyStringParam("a");
    Assert.assertEquals(0.4, testParams.getMyDoubleParam(), 0.0);
    Assert.assertEquals("a", testParams.getMyStringParam());
    Assert.assertArrayEquals(testParams.getMyDoubleArrayParam(), new double[]{1.0, 2.0}, 0.0);
  }

  @Test
  public void testParamValidate() {
    ParamValidators.gt(1.0);
    ParamValidators.gtEq(1.0);
    ParamValidators.lt(1.0);
    ParamValidators.ltEq(1.0);
    ParamValidators.inRange(0, 1, true, false);
    ParamValidators.inRange(0, 1);
    ParamValidators.inArray(Arrays.asList(0, 1, 3));
    ParamValidators.inArray(Arrays.asList("a", "b"));
  }
}
