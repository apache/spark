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
package org.apache.hadoop.net;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import junit.framework.TestCase;

public class TestScriptBasedMapping extends TestCase {

  public void testNoArgsMeansNoResult() {
    ScriptBasedMapping mapping = new ScriptBasedMapping();

    Configuration conf = new Configuration();
    conf.setInt(ScriptBasedMapping.SCRIPT_ARG_COUNT_KEY,
        ScriptBasedMapping.MIN_ALLOWABLE_ARGS - 1);
    conf.set(ScriptBasedMapping.SCRIPT_FILENAME_KEY, "any-filename");

    mapping.setConf(conf);

    List<String> names = new ArrayList<String>();
    names.add("some.machine.name");
    names.add("other.machine.name");

    List<String> result = mapping.resolve(names);
    assertNull(result);
  }
}
