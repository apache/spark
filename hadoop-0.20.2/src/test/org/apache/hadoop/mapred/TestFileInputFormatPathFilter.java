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

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.io.Writer;
import java.io.OutputStreamWriter;
import java.util.Set;
import java.util.HashSet;

public class TestFileInputFormatPathFilter extends TestCase {

  public static class DummyFileInputFormat extends FileInputFormat {

    public RecordReader getRecordReader(InputSplit split, JobConf job,
                                        Reporter reporter) throws IOException {
      return null;
    }

  }

  private static FileSystem localFs = null;

  static {
    try {
      localFs = FileSystem.getLocal(new JobConf());
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir =
      new Path(new Path(System.getProperty("test.build.data", "."), "data"),
          "TestFileInputFormatPathFilter");


  public void setUp() throws Exception {
    tearDown();
    localFs.mkdirs(workDir);
  }

  public void tearDown() throws Exception {
    if (localFs.exists(workDir)) {
      localFs.delete(workDir, true);
    }
  }

  protected Path createFile(String fileName) throws IOException {
    Path file = new Path(workDir, fileName);
    Writer writer = new OutputStreamWriter(localFs.create(file));
    writer.write("");
    writer.close();
    return localFs.makeQualified(file);
  }

  protected Set<Path> createFiles() throws IOException {
    Set<Path> files = new HashSet<Path>();
    files.add(createFile("a"));
    files.add(createFile("b"));
    files.add(createFile("aa"));
    files.add(createFile("bb"));
    files.add(createFile("_hello"));
    files.add(createFile(".hello"));
    return files;
  }


  public static class TestPathFilter implements PathFilter {

    public boolean accept(Path path) {
      String name = path.getName();
      return name.equals("TestFileInputFormatPathFilter") || name.length() == 1;
    }
  }

  private void _testInputFiles(boolean withFilter, boolean withGlob) throws Exception {
    Set<Path> createdFiles = createFiles();
    JobConf conf = new JobConf();

    Path inputDir = (withGlob) ? new Path(workDir, "a*") : workDir;
    FileInputFormat.setInputPaths(conf, inputDir);
    conf.setInputFormat(DummyFileInputFormat.class);

    if (withFilter) {
      FileInputFormat.setInputPathFilter(conf, TestPathFilter.class);
    }

    DummyFileInputFormat inputFormat =
        (DummyFileInputFormat) conf.getInputFormat();
    Set<Path> computedFiles = new HashSet<Path>();
    for (FileStatus file : inputFormat.listStatus(conf)) {
      computedFiles.add(file.getPath());
    }

    createdFiles.remove(localFs.makeQualified(new Path(workDir, "_hello")));
    createdFiles.remove(localFs.makeQualified(new Path(workDir, ".hello")));
    
    if (withFilter) {
      createdFiles.remove(localFs.makeQualified(new Path(workDir, "aa")));
      createdFiles.remove(localFs.makeQualified(new Path(workDir, "bb")));
    }

    if (withGlob) {
      createdFiles.remove(localFs.makeQualified(new Path(workDir, "b")));
      createdFiles.remove(localFs.makeQualified(new Path(workDir, "bb")));
    }
    assertEquals(createdFiles, computedFiles);
  }

  public void testWithoutPathFilterWithoutGlob() throws Exception {
    _testInputFiles(false, false);
  }

  public void testWithoutPathFilterWithGlob() throws Exception {
    _testInputFiles(false, true);
  }

  public void testWithPathFilterWithoutGlob() throws Exception {
    _testInputFiles(true, false);
  }

  public void testWithPathFilterWithGlob() throws Exception {
    _testInputFiles(true, true);
  }
}
