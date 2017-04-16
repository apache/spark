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
package org.apache.hadoop.record.compiler.ant;

import java.io.File;
import java.util.ArrayList;
import org.apache.hadoop.record.compiler.generated.Rcc;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.types.FileSet;

/**
 * Hadoop record compiler ant Task
 *<p> This task takes the given record definition files and compiles them into
 * java or c++
 * files. It is then up to the user to compile the generated files.
 *
 * <p> The task requires the <code>file</code> or the nested fileset element to be
 * specified. Optional attributes are <code>language</code> (set the output
 * language, default is "java"),
 * <code>destdir</code> (name of the destination directory for generated java/c++
 * code, default is ".") and <code>failonerror</code> (specifies error handling
 * behavior. default is true).
 * <p><h4>Usage</h4>
 * <pre>
 * &lt;recordcc
 *       destdir="${basedir}/gensrc"
 *       language="java"&gt;
 *   &lt;fileset include="**\/*.jr" /&gt;
 * &lt;/recordcc&gt;
 * </pre>
 */
public class RccTask extends Task {
  
  private String language = "java";
  private File src;
  private File dest = new File(".");
  private final ArrayList<FileSet> filesets = new ArrayList<FileSet>();
  private boolean failOnError = true;
  
  /** Creates a new instance of RccTask */
  public RccTask() {
  }
  
  /**
   * Sets the output language option
   * @param language "java"/"c++"
   */
  public void setLanguage(String language) {
    this.language = language;
  }
  
  /**
   * Sets the record definition file attribute
   * @param file record definition file
   */
  public void setFile(File file) {
    this.src = file;
  }
  
  /**
   * Given multiple files (via fileset), set the error handling behavior
   * @param flag true will throw build exception in case of failure (default)
   */
  public void setFailonerror(boolean flag) {
    this.failOnError = flag;
  }
  
  /**
   * Sets directory where output files will be generated
   * @param dir output directory
   */
  public void setDestdir(File dir) {
    this.dest = dir;
  }
  
  /**
   * Adds a fileset that can consist of one or more files
   * @param set Set of record definition files
   */
  public void addFileset(FileSet set) {
    filesets.add(set);
  }
  
  /**
   * Invoke the Hadoop record compiler on each record definition file
   */
  public void execute() throws BuildException {
    if (src == null && filesets.size()==0) {
      throw new BuildException("There must be a file attribute or a fileset child element");
    }
    if (src != null) {
      doCompile(src);
    }
    Project myProject = getProject();
    for (int i = 0; i < filesets.size(); i++) {
      FileSet fs = filesets.get(i);
      DirectoryScanner ds = fs.getDirectoryScanner(myProject);
      File dir = fs.getDir(myProject);
      String[] srcs = ds.getIncludedFiles();
      for (int j = 0; j < srcs.length; j++) {
        doCompile(new File(dir, srcs[j]));
      }
    }
  }
  
  private void doCompile(File file) throws BuildException {
    String[] args = new String[5];
    args[0] = "--language";
    args[1] = this.language;
    args[2] = "--destdir";
    args[3] = this.dest.getPath();
    args[4] = file.getPath();
    int retVal = Rcc.driver(args);
    if (retVal != 0 && failOnError) {
      throw new BuildException("Hadoop record compiler returned error code "+retVal);
    }
  }
}
