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

package org.apache.spark.network.shuffle;

import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.util.JavaUtils;

import static org.apache.spark.network.shuffle.ExternalShuffleMessages.*;

public class ShuffleMessagesSuite {
  @Test
  public void serializeOpenShuffleBlocks() {
    OpenShuffleBlocks msg = new OpenShuffleBlocks("app-1", "exec-2",
      new String[] { "block0", "block1" });
    OpenShuffleBlocks msg2 = JavaUtils.deserialize(JavaUtils.serialize(msg));
    assertEquals(msg, msg2);
  }

  @Test
  public void serializeRegisterExecutor() {
    RegisterExecutor msg = new RegisterExecutor("app-1", "exec-2", new ExecutorShuffleInfo(
      new String[] { "/local1", "/local2" }, 32, "MyShuffleManager"));
    RegisterExecutor msg2 = JavaUtils.deserialize(JavaUtils.serialize(msg));
    assertEquals(msg, msg2);
  }

  @Test
  public void serializeShuffleStreamHandle() {
    ShuffleStreamHandle msg = new ShuffleStreamHandle(12345, 16);
    ShuffleStreamHandle msg2 = JavaUtils.deserialize(JavaUtils.serialize(msg));
    assertEquals(msg, msg2);
  }
}
