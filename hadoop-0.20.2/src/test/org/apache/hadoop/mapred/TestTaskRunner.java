/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestTaskRunner {
  /**
   * Test that environment variables are properly escaped when exported from
   * taskjvm.sh
   */
  @Test
  public void testEnvironmentEscaping() {
    Map<String,String> env = new TreeMap<String, String>();
    env.put("BAZ", "blah blah multiple words");
    env.put("FOO", "bar");
    env.put("QUOTED", "bad chars like \\ and \"");

    List<String> exportCmds = new ArrayList<String>();
    TaskRunner.appendEnvExports(exportCmds, env);
    assertEquals(3, exportCmds.size());
    assertEquals("export BAZ=\"blah blah multiple words\"",
                 exportCmds.get(0));
    assertEquals("export FOO=\"bar\"",
                 exportCmds.get(1));
    assertEquals("export QUOTED=\"bad chars like \\\\ and \\\"\"",
                 exportCmds.get(2));
  }
}