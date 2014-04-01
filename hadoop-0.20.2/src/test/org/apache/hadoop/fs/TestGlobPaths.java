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
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import junit.framework.TestCase;

public class TestGlobPaths extends TestCase {
  
  static class RegexPathFilter implements PathFilter {
    
    private final String regex;
    public RegexPathFilter(String regex) {
      this.regex = regex;
    }

    public boolean accept(Path path) {
      return path.toString().matches(regex);
    }

  }
  
  static private MiniDFSCluster dfsCluster;
  static private FileSystem fs;
  static final private int NUM_OF_PATHS = 4;
  static final String USER_DIR = "/user/"+System.getProperty("user.name");
  private Path[] path = new Path[NUM_OF_PATHS];
  
  protected void setUp() throws Exception {
    try {
      Configuration conf = new Configuration();
      dfsCluster = new MiniDFSCluster(conf, 1, true, null);
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  protected void tearDown() throws Exception {
    if(dfsCluster!=null) {
      dfsCluster.shutdown();
    }
  }
  
  public void testPathFilter() throws IOException {
    try {
      String[] files = new String[] { USER_DIR + "/a", USER_DIR + "/a/b" };
      Path[] matchedPath = prepareTesting(USER_DIR + "/*/*", files,
          new RegexPathFilter("^.*" + Pattern.quote(USER_DIR) + "/a/b"));
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  public void testPathFilterWithFixedLastComponent() throws IOException {
    try {
      String[] files = new String[] { USER_DIR + "/a", USER_DIR + "/a/b",
                                      USER_DIR + "/c", USER_DIR + "/c/b", };
      Path[] matchedPath = prepareTesting(USER_DIR + "/*/b", files,
          new RegexPathFilter("^.*" + Pattern.quote(USER_DIR) + "/a/b"));
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  public void testGlob() throws Exception {
    //pTestEscape(); // need to wait until HADOOP-1995 is fixed
    pTestJavaRegexSpecialChars();
    pTestCurlyBracket();
    pTestLiteral();
    pTestAny();
    pTestClosure();
    pTestSet();
    pTestRange();
    pTestSetExcl();
    pTestCombination();
    pTestRelativePath();
  }
  
  private void pTestLiteral() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/a2c", USER_DIR+"/abc.d"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/abc.d", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestEscape() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/ab\\[c.d"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/ab\\[c.d", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestAny() throws IOException {
    try {
      String [] files = new String[] { USER_DIR+"/abc", USER_DIR+"/a2c",
                                       USER_DIR+"/a.c", USER_DIR+"/abcd"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a?c", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[2]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[0]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestClosure() throws IOException {
    pTestClosure1();
    pTestClosure2();
    pTestClosure3();
    pTestClosure4();
    pTestClosure5();
  }
  
  private void pTestClosure1() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/a", USER_DIR+"/abc",
                                      USER_DIR+"/abc.p", USER_DIR+"/bacd"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a*", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[2]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestClosure2() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/a.", USER_DIR+"/a.txt",
                                     USER_DIR+"/a.old.java", USER_DIR+"/.java"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.*", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[2]);
      assertEquals(matchedPath[2], path[1]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestClosure3() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.txt.x", USER_DIR+"/ax",
                                      USER_DIR+"/ab37x", USER_DIR+"/bacd"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a*x", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[2]);
      assertEquals(matchedPath[2], path[1]);
    } finally {
      cleanupDFS();
    } 
  }

  private void pTestClosure4() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/dir1/file1", 
                                      USER_DIR+"/dir2/file2", 
                                       USER_DIR+"/dir3/file1"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/*/file1", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[2]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestClosure5() throws IOException {
    try {
      String [] files = new String[] {USER_DIR+"/dir1/file1", 
                                      USER_DIR+"/file1"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/*/file1", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
    } finally {
      cleanupDFS();
    }
  }

  private void pTestSet() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.c", USER_DIR+"/a.cpp",
                                      USER_DIR+"/a.hlp", USER_DIR+"/a.hxy"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[ch]??", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[1]);
      assertEquals(matchedPath[1], path[2]);
      assertEquals(matchedPath[2], path[3]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestRange() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.d", USER_DIR+"/a.e",
                                      USER_DIR+"/a.f", USER_DIR+"/a.h"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[d-fm]", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[2]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestSetExcl() throws IOException {
    try {    
      String [] files = new String[] {USER_DIR+"/a.d", USER_DIR+"/a.e",
                                      USER_DIR+"/a.0", USER_DIR+"/a.h"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/a.[^a-cg-z0-9]", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
    } finally {
      cleanupDFS();
    }
  }

  private void pTestCombination() throws IOException {
    try {    
      String [] files = new String[] {"/user/aa/a.c", "/user/bb/a.cpp",
                                      "/user1/cc/b.hlp", "/user/dd/a.hxy"};
      Path[] matchedPath = prepareTesting("/use?/*/a.[ch]{lp,xy}", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[3]);
    } finally {
      cleanupDFS();
    }
  }
  
  private void pTestRelativePath() throws IOException {
    try {
      String [] files = new String[] {"a", "abc", "abc.p", "bacd"};
      Path[] matchedPath = prepareTesting("a*", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], new Path(USER_DIR, path[0]));
      assertEquals(matchedPath[1], new Path(USER_DIR, path[1]));
      assertEquals(matchedPath[2], new Path(USER_DIR, path[2]));
    } finally {
      cleanupDFS();
    }
  }
  
  /* Test {xx,yy} */
  private void pTestCurlyBracket() throws IOException {
    Path[] matchedPath;
    String [] files;
    try {
      files = new String[] { USER_DIR+"/a.abcxx", USER_DIR+"/a.abxy",
                             USER_DIR+"/a.hlp", USER_DIR+"/a.jhyy"};
      matchedPath = prepareTesting(USER_DIR+"/a.{abc,jh}??", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[3]);
    } finally {
      cleanupDFS();
    }
    // nested curlies
    try {
      files = new String[] { USER_DIR+"/a.abcxx", USER_DIR+"/a.abdxy",
                             USER_DIR+"/a.hlp", USER_DIR+"/a.jhyy" };
      matchedPath = prepareTesting(USER_DIR+"/a.{ab{c,d},jh}??", files);
      assertEquals(matchedPath.length, 3);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);
      assertEquals(matchedPath[2], path[3]);
    } finally {
      cleanupDFS();
    }
    // cross-component curlies
    try {
      files = new String[] { USER_DIR+"/a/b", USER_DIR+"/a/d",
                             USER_DIR+"/c/b", USER_DIR+"/c/d" };
      matchedPath = prepareTesting(USER_DIR+"/{a/b,c/d}", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[3]);
    } finally {
      cleanupDFS();
    }
    // cross-component absolute curlies
    try {
      files = new String[] { "/a/b", "/a/d",
                             "/c/b", "/c/d" };
      matchedPath = prepareTesting("{/a/b,/c/d}", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[3]);
    } finally {
      cleanupDFS();
    }
    try {
      // test standalone }
      files = new String[] {USER_DIR+"/}bc", USER_DIR+"/}c"};
      matchedPath = prepareTesting(USER_DIR+"/}{a,b}c", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
      // test {b}
      matchedPath = prepareTesting(USER_DIR+"/}{b}c", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
      // test {}
      matchedPath = prepareTesting(USER_DIR+"/}{}bc", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);

      // test {,}
      matchedPath = prepareTesting(USER_DIR+"/}{,}bc", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);

      // test {b,}
      matchedPath = prepareTesting(USER_DIR+"/}{b,}c", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);

      // test {,b}
      matchedPath = prepareTesting(USER_DIR+"/}{,b}c", files);
      assertEquals(matchedPath.length, 2);
      assertEquals(matchedPath[0], path[0]);
      assertEquals(matchedPath[1], path[1]);

      // test a combination of {} and ?
      matchedPath = prepareTesting(USER_DIR+"/}{ac,?}", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[1]);
      
      // test ill-formed curly
      boolean hasException = false;
      try {
        prepareTesting(USER_DIR+"}{bc", files);
      } catch (IOException e) {
        assertTrue(e.getMessage().startsWith("Illegal file pattern:") );
        hasException = true;
      }
      assertTrue(hasException);
    } finally {
      cleanupDFS();
    }
  }
  
  /* test that a path name can contain Java regex special characters */
  private void pTestJavaRegexSpecialChars() throws IOException {
    try {
      String[] files = new String[] {USER_DIR+"/($.|+)bc", USER_DIR+"/abc"};
      Path[] matchedPath = prepareTesting(USER_DIR+"/($.|+)*", files);
      assertEquals(matchedPath.length, 1);
      assertEquals(matchedPath[0], path[0]);
    } finally {
      cleanupDFS();
    }

  }
  private Path[] prepareTesting(String pattern, String[] files)
    throws IOException {
    for(int i=0; i<Math.min(NUM_OF_PATHS, files.length); i++) {
      path[i] = new Path(files[i]).makeQualified(fs);
      if (!fs.mkdirs(path[i])) {
        throw new IOException("Mkdirs failed to create " + path[i].toString());
      }
    }
    Path patternPath = new Path(pattern);
    Path[] globResults = FileUtil.stat2Paths(fs.globStatus(patternPath),
                                             patternPath);
    for(int i=0; i<globResults.length; i++) {
      globResults[i] = globResults[i].makeQualified(fs);
    }
    return globResults;
  }
  
  private Path[] prepareTesting(String pattern, String[] files,
      PathFilter filter) throws IOException {
    for(int i=0; i<Math.min(NUM_OF_PATHS, files.length); i++) {
      path[i] = new Path(files[i]).makeQualified(fs);
      if (!fs.mkdirs(path[i])) {
        throw new IOException("Mkdirs failed to create " + path[i].toString());
      }
    }
    Path patternPath = new Path(pattern);
    Path[] globResults = FileUtil.stat2Paths(fs.globStatus(patternPath, filter),
                                             patternPath);
    for(int i=0; i<globResults.length; i++) {
      globResults[i] = globResults[i].makeQualified(fs);
    }
    return globResults;
  }
  
  private void cleanupDFS() throws IOException {
    fs.delete(new Path("/user"), true);
  }
  
}
