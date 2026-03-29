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
package org.apache.spark.sql.catalyst.expressions;

import java.io.IOException;
import java.io.Serializable;

public class TestThrowExceptionMethod implements Serializable {

  public int invoke(int i) throws IOException {
    if (i != 0) {
      return i * 2;
    } else {
      throw new IOException("Invoke the method that throw IOException");
    }
  }

  public static int staticInvoke(int i) throws IOException {
    if (i != 0) {
      return i * 2;
    } else {
      throw new IOException("StaticInvoke the method that throw IOException");
    }
  }
}
