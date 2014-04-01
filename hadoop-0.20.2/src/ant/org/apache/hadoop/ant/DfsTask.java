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

package org.apache.hadoop.ant;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;

import org.apache.tools.ant.AntClassLoader;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.types.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * {@link org.apache.hadoop.fs.FsShell FsShell} wrapper for ant Task.
 */
public class DfsTask extends Task {

  /**
   * Default sink for {@link java.lang.System.out System.out}
   * and {@link java.lang.System.err System.err}.
   */
  private static final OutputStream nullOut = new OutputStream() {
      public void write(int b)    { /* ignore */ }
      public String toString()    { return ""; }
  };
  private static final FsShell shell = new FsShell();

  protected AntClassLoader confloader;
  protected OutputStream out = nullOut;
  protected OutputStream err = nullOut;

  // set by ant
  protected String cmd;
  protected final LinkedList<String> argv = new LinkedList<String>();
  protected String outprop;
  protected String errprop;
  protected boolean failonerror = true;

  // saved ant context
  private PrintStream antOut;
  private PrintStream antErr;

  /**
   * Sets the command to run in {@link org.apache.hadoop.fs.FsShell FsShell}.
   * @param cmd A valid command to FsShell, sans &quot;-&quot;.
   */
  public void setCmd(String cmd) {
    this.cmd = "-" + cmd.trim();
  }

  /**
   * Sets the argument list from a String of comma-separated values.
   * @param args A String of comma-separated arguments to FsShell.
   */
  public void setArgs(String args) {
    for (String s : args.trim().split("\\s*,\\s*"))
      argv.add(s);
  }

  /**
   * Sets the property into which System.out will be written.
   * @param outprop The name of the property into which System.out is written.
   * If the property is defined before this task is executed, it will not be updated.
   */
  public void setOut(String outprop) {
    this.outprop = outprop;
    out = new ByteArrayOutputStream();
    if (outprop.equals(errprop))
      err = out;
  }

  /**
   * Sets the property into which System.err will be written. If this property
   * has the same name as the property for System.out, the two will be interlaced.
   * @param errprop The name of the property into which System.err is written.
   * If the property is defined before this task is executed, it will not be updated.
   */
  public void setErr(String errprop) {
    this.errprop = errprop;
    err = (errprop.equals(outprop)) ? err = out : new ByteArrayOutputStream();
  }

  /**
   * Sets the path for the parent-last ClassLoader, intended to be used for
   * {@link org.apache.hadoop.conf.Configuration Configuration}.
   * @param confpath The path to search for resources, classes, etc. before
   * parent ClassLoaders.
   */
  public void setConf(String confpath) {
    confloader = new AntClassLoader(getClass().getClassLoader(), false);
    confloader.setProject(getProject());
    if (null != confpath)
      confloader.addPathElement(confpath);
  }

  /**
   * Sets a property controlling whether or not a
   * {@link org.apache.tools.ant.BuildException BuildException} will be thrown
   * if the command returns a value less than zero or throws an exception.
   * @param failonerror If true, throw a BuildException on error.
   */
  public void setFailonerror(boolean failonerror) {
    this.failonerror = failonerror;
  }

  /**
   * Save the current values of System.out, System.err and configure output
   * streams for FsShell.
   */
  protected void pushContext() {
    antOut = System.out;
    antErr = System.err;
    System.setOut(new PrintStream(out));
    System.setErr(out == err ? System.out : new PrintStream(err));
  }

  /**
   * Create the appropriate output properties with their respective output,
   * restore System.out, System.err and release any resources from created
   * ClassLoaders to aid garbage collection.
   */
  protected void popContext() {
    // write output to property, if applicable
    if (outprop != null && !System.out.checkError())
      getProject().setNewProperty(outprop, out.toString());
    if (out != err && errprop != null && !System.err.checkError())
      getProject().setNewProperty(errprop, err.toString());

    System.setErr(antErr);
    System.setOut(antOut);
    confloader.cleanup();
    confloader.setParent(null);
  }

  // in case DfsTask is overridden
  protected int postCmd(int exit_code) {
    if ("-test".equals(cmd) && exit_code != 0)
      outprop = null;
    return exit_code;
  }

  /**
   * Invoke {@link org.apache.hadoop.fs.FsShell#doMain FsShell.doMain} after a
   * few cursory checks of the configuration.
   */
  public void execute() throws BuildException {
    if (null == cmd)
      throw new BuildException("Missing command (cmd) argument");
    argv.add(0, cmd);

    if (null == confloader) {
      setConf(getProject().getProperty("hadoop.conf.dir"));
    }

    int exit_code = 0;
    try {
      pushContext();

      Configuration conf = new Configuration();
      conf.setClassLoader(confloader);
      exit_code = ToolRunner.run(conf, shell,
          argv.toArray(new String[argv.size()]));
      exit_code = postCmd(exit_code);

      if (0 > exit_code) {
        StringBuilder msg = new StringBuilder();
        for (String s : argv)
          msg.append(s + " ");
        msg.append("failed: " + exit_code);
        throw new Exception(msg.toString());
      }
    } catch (Exception e) {
      if (failonerror)
          throw new BuildException(e);
    } finally {
      popContext();
    }
  }
}
