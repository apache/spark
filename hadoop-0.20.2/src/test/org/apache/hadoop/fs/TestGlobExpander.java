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
package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

public class TestGlobExpander extends TestCase {

  public void testExpansionIsIdentical() throws IOException {
    checkExpansionIsIdentical("");
    checkExpansionIsIdentical("/}");
    checkExpansionIsIdentical("/}{a,b}");
    checkExpansionIsIdentical("{/");
    checkExpansionIsIdentical("{a}");
    checkExpansionIsIdentical("{a,b}/{b,c}");
    checkExpansionIsIdentical("p\\{a/b,c/d\\}s");
    checkExpansionIsIdentical("p{a\\/b,c\\/d}s");
  }

  public void testExpansion() throws IOException {
    checkExpansion("{a/b}", "a/b");
    checkExpansion("/}{a/b}", "/}a/b");
    checkExpansion("p{a/b,c/d}s", "pa/bs", "pc/ds");
    checkExpansion("{a/b,c/d,{e,f}}", "a/b", "c/d", "{e,f}");
    checkExpansion("{a/b,c/d}{e,f}", "a/b{e,f}", "c/d{e,f}");
    checkExpansion("{a,b}/{b,{c/d,e/f}}", "{a,b}/b", "{a,b}/c/d", "{a,b}/e/f");
    checkExpansion("{a,b}/{c/\\d}", "{a,b}/c/d");
  }

  private void checkExpansionIsIdentical(String filePattern) throws IOException {
    checkExpansion(filePattern, filePattern);
  }

  private void checkExpansion(String filePattern, String... expectedExpansions)
      throws IOException {
    List<String> actualExpansions = GlobExpander.expand(filePattern);
    assertEquals("Different number of expansions", expectedExpansions.length,
        actualExpansions.size());
    for (int i = 0; i < expectedExpansions.length; i++) {
      assertEquals("Expansion of " + filePattern, expectedExpansions[i],
          actualExpansions.get(i));
    }
  }
}
