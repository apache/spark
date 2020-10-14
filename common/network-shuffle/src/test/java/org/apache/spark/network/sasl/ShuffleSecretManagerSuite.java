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

package org.apache.spark.network.sasl;

import java.nio.ByteBuffer;

import org.junit.Test;
import static org.junit.Assert.*;

public class ShuffleSecretManagerSuite {
  static String app1 = "app1";
  static String app2 = "app2";
  static String pw1 = "password1";
  static String pw2 = "password2";
  static String pw1update = "password1update";
  static String pw2update = "password2update";

  @Test
  public void testMultipleRegisters() {
    ShuffleSecretManager secretManager = new ShuffleSecretManager();
    secretManager.registerApp(app1, pw1);
    assertEquals(pw1, secretManager.getSecretKey(app1));
    secretManager.registerApp(app2, ByteBuffer.wrap(pw2.getBytes()));
    assertEquals(pw2, secretManager.getSecretKey(app2));

    // now update the password for the apps and make sure it takes affect
    secretManager.registerApp(app1, pw1update);
    assertEquals(pw1update, secretManager.getSecretKey(app1));
    secretManager.registerApp(app2, ByteBuffer.wrap(pw2update.getBytes()));
    assertEquals(pw2update, secretManager.getSecretKey(app2));

    secretManager.unregisterApp(app1);
    assertNull(secretManager.getSecretKey(app1));
    assertEquals(pw2update, secretManager.getSecretKey(app2));

    secretManager.unregisterApp(app2);
    assertNull(secretManager.getSecretKey(app2));
    assertNull(secretManager.getSecretKey(app1));
  }
}
