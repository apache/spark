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

package org.apache.spark;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.ApplicationId;

public class ApplicationIdSuite {

  private ApplicationId appId1_1, appId1_2, appId1_3, appId2, appIdNull_1, appIdNull_2;

  @Before
  public void setUp() {
    appId1_1 = new ApplicationId("appId1");
    appId1_2 = new ApplicationId("appId1");
    appId1_3 = new ApplicationId(new String("appId1"));
    appId2 = new ApplicationId("appId2");
    appIdNull_1 = new ApplicationId(null);
    appIdNull_2 = new ApplicationId(null);
  }

  @Test
  public void testEquality() {
    // When ID strings are same object, ApplicationIds are equivalent
    assertThat(appId1_1.toString(), is(sameInstance(appId1_2.toString())));
    assertThat(appId1_1.equals(appId1_2), is(true));

    // When ID strings are not same object but equivalent, ApplicationIds are equivalent
    assertThat(appId1_1.toString(), is(not(sameInstance(appId1_3.toString()))));
    assertThat(appId1_1.toString(), is(appId1_3.toString()));
    assertThat(appId1_1.equals(appId1_3), is(true));

    // When ID strings are not equivalent, ApplicationIds are not equivalent
    assertThat(appId1_1.toString(), is(not(appId2.toString())));
    assertThat(appId1_1.equals(appId2), is(false));

    // When one of ApplicationIds has null ID, ApplicationIds are not equivalent
    assertThat(appIdNull_1.toString(), is(nullValue()));
    assertThat(appIdNull_2.toString(), is(nullValue()));
    assertThat(appId1_1.equals(appIdNull_1), is(false));
    assertThat(appIdNull_1.equals(appIdNull_2), is(false));
  }

  @Test
  public void testEqualsMethodSpecification() {
    // Test reflexivity rule
    assertThat(appId1_1.equals(appId1_1), is(true));

    // Test transitivity rule
    assertThat(appId1_1.equals(appId1_2), is(true));
    assertThat(appId1_2.equals(appId1_3), is(true));
    assertThat(appId1_1.equals(appId1_3), is(true));

    // Test symmetric rule
    assertThat(appId1_2.equals(appId1_1), is(true));
    assertThat(appId1_3.equals(appId1_2), is(true));
    assertThat(appId1_3.equals(appId1_1), is(true));

    // Test consistency rule
    assertThat(appId1_1.equals(appId1_2), is(appId1_1.equals(appId1_2)));
    assertThat(appId1_3.equals(appId2), is(appId1_3.equals(appId2)));

    // Test comparision with null
    assertThat(appId1_1.equals(null), is(false));
  }

}