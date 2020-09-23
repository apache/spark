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

import java.io.IOException;

import com.google.common.base.Throwables;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.spark.network.shuffle.protocol.PushBlockStream;
import org.apache.spark.network.util.JavaUtils;

public class BlockPushExceptionSuite {

  @Test
  public void testDecodeExceptionMsg() {
    PushBlockStream header = new PushBlockStream("app_0", "block_0", 128);
    IOException ioexp = new IOException("Test exception");
    String encodedMsg = JavaUtils.encodeHeaderIntoErrorString(header.toByteBuffer(), ioexp);
    BlockPushException exp = BlockPushException.decodeException(encodedMsg);
    assertEquals(header, exp.getHeader());
    assertEquals(Throwables.getStackTraceAsString(ioexp), exp.getMessage());
  }
}
