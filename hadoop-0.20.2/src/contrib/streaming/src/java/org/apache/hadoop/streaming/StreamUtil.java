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

package org.apache.hadoop.streaming;

import java.text.DecimalFormat;
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;

/** Utilities not available elsewhere in Hadoop.
 *  
 */
public class StreamUtil {

  /** It may seem strange to silently switch behaviour when a String
   * is not a classname; the reason is simplified Usage:<pre>
   * -mapper [classname | program ]
   * instead of the explicit Usage:
   * [-mapper program | -javamapper classname], -mapper and -javamapper are mutually exclusive.
   * (repeat for -reducer, -combiner) </pre>
   */
  public static Class goodClassOrNull(Configuration conf, String className, String defaultPackage) {
    if (className.indexOf('.') == -1 && defaultPackage != null) {
      className = defaultPackage + "." + className;
    }
    Class clazz = null;
    try {
      clazz = conf.getClassByName(className);
    } catch (ClassNotFoundException cnf) {
    }
    return clazz;
  }

  public static String findInClasspath(String className) {
    return findInClasspath(className, StreamUtil.class.getClassLoader());
  }

  /** @return a jar file path or a base directory or null if not found.
   */
  public static String findInClasspath(String className, ClassLoader loader) {

    String relPath = className;
    relPath = relPath.replace('.', '/');
    relPath += ".class";
    java.net.URL classUrl = loader.getResource(relPath);

    String codePath;
    if (classUrl != null) {
      boolean inJar = classUrl.getProtocol().equals("jar");
      codePath = classUrl.toString();
      if (codePath.startsWith("jar:")) {
        codePath = codePath.substring("jar:".length());
      }
      if (codePath.startsWith("file:")) { // can have both
        codePath = codePath.substring("file:".length());
      }
      if (inJar) {
        // A jar spec: remove class suffix in /path/my.jar!/package/Class
        int bang = codePath.lastIndexOf('!');
        codePath = codePath.substring(0, bang);
      } else {
        // A class spec: remove the /my/package/Class.class portion
        int pos = codePath.lastIndexOf(relPath);
        if (pos == -1) {
          throw new IllegalArgumentException("invalid codePath: className=" + className
                                             + " codePath=" + codePath);
        }
        codePath = codePath.substring(0, pos);
      }
    } else {
      codePath = null;
    }
    return codePath;
  }

  // copied from TaskRunner  
  static void unJar(File jarFile, File toDir) throws IOException {
    JarFile jar = new JarFile(jarFile);
    try {
      Enumeration entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = (JarEntry) entries.nextElement();
        if (!entry.isDirectory()) {
          InputStream in = jar.getInputStream(entry);
          try {
            File file = new File(toDir, entry.getName());
            file.getParentFile().mkdirs();
            OutputStream out = new FileOutputStream(file);
            try {
              byte[] buffer = new byte[8192];
              int i;
              while ((i = in.read(buffer)) != -1) {
                out.write(buffer, 0, i);
              }
            } finally {
              out.close();
            }
          } finally {
            in.close();
          }
        }
      }
    } finally {
      jar.close();
    }
  }

  final static long KB = 1024L * 1;
  final static long MB = 1024L * KB;
  final static long GB = 1024L * MB;
  final static long TB = 1024L * GB;
  final static long PB = 1024L * TB;

  static DecimalFormat dfm = new DecimalFormat("####.000");
  static DecimalFormat ifm = new DecimalFormat("###,###,###,###,###");

  public static String dfmt(double d) {
    return dfm.format(d);
  }

  public static String ifmt(double d) {
    return ifm.format(d);
  }

  public static String formatBytes(long numBytes) {
    StringBuffer buf = new StringBuffer();
    boolean bDetails = true;
    double num = numBytes;

    if (numBytes < KB) {
      buf.append(numBytes).append(" B");
      bDetails = false;
    } else if (numBytes < MB) {
      buf.append(dfmt(num / KB)).append(" KB");
    } else if (numBytes < GB) {
      buf.append(dfmt(num / MB)).append(" MB");
    } else if (numBytes < TB) {
      buf.append(dfmt(num / GB)).append(" GB");
    } else if (numBytes < PB) {
      buf.append(dfmt(num / TB)).append(" TB");
    } else {
      buf.append(dfmt(num / PB)).append(" PB");
    }
    if (bDetails) {
      buf.append(" (").append(ifmt(numBytes)).append(" bytes)");
    }
    return buf.toString();
  }

  public static String formatBytes2(long numBytes) {
    StringBuffer buf = new StringBuffer();
    long u = 0;
    if (numBytes >= TB) {
      u = numBytes / TB;
      numBytes -= u * TB;
      buf.append(u).append(" TB ");
    }
    if (numBytes >= GB) {
      u = numBytes / GB;
      numBytes -= u * GB;
      buf.append(u).append(" GB ");
    }
    if (numBytes >= MB) {
      u = numBytes / MB;
      numBytes -= u * MB;
      buf.append(u).append(" MB ");
    }
    if (numBytes >= KB) {
      u = numBytes / KB;
      numBytes -= u * KB;
      buf.append(u).append(" KB ");
    }
    buf.append(u).append(" B"); //even if zero
    return buf.toString();
  }

  static Environment env;
  static String HOST;

  static {
    try {
      env = new Environment();
      HOST = env.getHost();
    } catch (IOException io) {
      io.printStackTrace();
    }
  }

  static class StreamConsumer extends Thread {

    StreamConsumer(InputStream in, OutputStream out) {
      this.bin = new LineNumberReader(new BufferedReader(new InputStreamReader(in)));
      if (out != null) {
        this.bout = new DataOutputStream(out);
      }
    }

    public void run() {
      try {
        String line;
        while ((line = bin.readLine()) != null) {
          if (bout != null) {
            bout.writeUTF(line); //writeChars
            bout.writeChar('\n');
          }
        }
        bout.flush();
      } catch (IOException io) {
      }
    }

    LineNumberReader bin;
    DataOutputStream bout;
  }

  static void exec(String arg, PrintStream log) {
    exec(new String[] { arg }, log);
  }

  static void exec(String[] args, PrintStream log) {
    try {
      log.println("Exec: start: " + Arrays.asList(args));
      Process proc = Runtime.getRuntime().exec(args);
      new StreamConsumer(proc.getErrorStream(), log).start();
      new StreamConsumer(proc.getInputStream(), log).start();
      int status = proc.waitFor();
      //if status != 0
      log.println("Exec: status=" + status + ": " + Arrays.asList(args));
    } catch (InterruptedException in) {
      in.printStackTrace();
    } catch (IOException io) {
      io.printStackTrace();
    }
  }

  static String qualifyHost(String url) {
    try {
      return qualifyHost(new URL(url)).toString();
    } catch (IOException io) {
      return url;
    }
  }

  static URL qualifyHost(URL url) {
    try {
      InetAddress a = InetAddress.getByName(url.getHost());
      String qualHost = a.getCanonicalHostName();
      URL q = new URL(url.getProtocol(), qualHost, url.getPort(), url.getFile());
      return q;
    } catch (IOException io) {
      return url;
    }
  }

  static final String regexpSpecials = "[]()?*+|.!^-\\~@";

  public static String regexpEscape(String plain) {
    StringBuffer buf = new StringBuffer();
    char[] ch = plain.toCharArray();
    int csup = ch.length;
    for (int c = 0; c < csup; c++) {
      if (regexpSpecials.indexOf(ch[c]) != -1) {
        buf.append("\\");
      }
      buf.append(ch[c]);
    }
    return buf.toString();
  }

  public static String safeGetCanonicalPath(File f) {
    try {
      String s = f.getCanonicalPath();
      return (s == null) ? f.toString() : s;
    } catch (IOException io) {
      return f.toString();
    }
  }

  static String slurp(File f) throws IOException {
    int len = (int) f.length();
    byte[] buf = new byte[len];
    FileInputStream in = new FileInputStream(f);
    String contents = null;
    try {
      in.read(buf, 0, len);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  static String slurpHadoop(Path p, FileSystem fs) throws IOException {
    int len = (int) fs.getLength(p);
    byte[] buf = new byte[len];
    FSDataInputStream in = fs.open(p);
    String contents = null;
    try {
      in.readFully(in.getPos(), buf);
      contents = new String(buf, "UTF-8");
    } finally {
      in.close();
    }
    return contents;
  }

  public static String rjustify(String s, int width) {
    if (s == null) s = "null";
    if (width > s.length()) {
      s = getSpace(width - s.length()) + s;
    }
    return s;
  }

  public static String ljustify(String s, int width) {
    if (s == null) s = "null";
    if (width > s.length()) {
      s = s + getSpace(width - s.length());
    }
    return s;
  }

  static char[] space;
  static {
    space = new char[300];
    Arrays.fill(space, '\u0020');
  }

  public static String getSpace(int len) {
    if (len > space.length) {
      space = new char[Math.max(len, 2 * space.length)];
      Arrays.fill(space, '\u0020');
    }
    return new String(space, 0, len);
  }

  static private Environment env_;

  static Environment env() {
    if (env_ != null) {
      return env_;
    }
    try {
      env_ = new Environment();
    } catch (IOException io) {
      io.printStackTrace();
    }
    return env_;
  }

  public static String makeJavaCommand(Class main, String[] argv) {
    ArrayList vargs = new ArrayList();
    File javaHomeBin = new File(System.getProperty("java.home"), "bin");
    File jvm = new File(javaHomeBin, "java");
    vargs.add(jvm.toString());
    // copy parent classpath
    vargs.add("-classpath");
    vargs.add("\"" + System.getProperty("java.class.path") + "\"");

    // add heap-size limit
    vargs.add("-Xmx" + Runtime.getRuntime().maxMemory());

    // Add main class and its arguments
    vargs.add(main.getName());
    for (int i = 0; i < argv.length; i++) {
      vargs.add(argv[i]);
    }
    return collate(vargs, " ");
  }

  public static String collate(Object[] args, String sep) {
    return collate(Arrays.asList(args), sep);
  }

  public static String collate(List args, String sep) {
    StringBuffer buf = new StringBuffer();
    Iterator it = args.iterator();
    while (it.hasNext()) {
      if (buf.length() > 0) {
        buf.append(" ");
      }
      buf.append(it.next());
    }
    return buf.toString();
  }

  // JobConf helpers

  public static FileSplit getCurrentSplit(JobConf job) {
    String path = job.get("map.input.file");
    if (path == null) {
      return null;
    }
    Path p = new Path(path);
    long start = Long.parseLong(job.get("map.input.start"));
    long length = Long.parseLong(job.get("map.input.length"));
    return new FileSplit(p, start, length, job);
  }

  static class TaskId {

    boolean mapTask;
    String jobid;
    int taskid;
    int execid;
  }

  public static boolean isLocalJobTracker(JobConf job) {
    return job.get("mapred.job.tracker", "local").equals("local");
  }

  public static TaskId getTaskInfo(JobConf job) {
    TaskId res = new TaskId();

    String id = job.get("mapred.task.id");
    if (isLocalJobTracker(job)) {
      // it uses difft naming 
      res.mapTask = job.getBoolean("mapred.task.is.map", true);
      res.jobid = "0";
      res.taskid = 0;
      res.execid = 0;
    } else {
      String[] e = id.split("_");
      res.mapTask = e[3].equals("m");
      res.jobid = e[1] + "_" + e[2];
      res.taskid = Integer.parseInt(e[4]);
      res.execid = Integer.parseInt(e[5]);
    }
    return res;
  }

  public static void touch(File file) throws IOException {
    file = file.getAbsoluteFile();
    FileOutputStream out = new FileOutputStream(file);
    out.close();
    if (!file.exists()) {
      throw new IOException("touch failed: " + file);
    }
  }

  public static boolean isCygwin() {
    String OS = System.getProperty("os.name");
    return (OS.indexOf("Windows") > -1);
  }

  public static String localizeBin(String path) {
    if (isCygwin()) {
      path = "C:/cygwin/" + path;
    }
    return path;
  }
  
  /** @param name foo where &lt;junit>&lt;sysproperty key="foo" value="${foo}"/> 
   * If foo is undefined then Ant sets the unevaluated value. 
   * Take this into account when setting defaultVal. */
  public static String getBoundAntProperty(String name, String defaultVal)
  {
    String val = System.getProperty(name);
    if (val != null && val.indexOf("${") >= 0) {
      val = null;
    }
    if (val == null) {
      val = defaultVal;
    }
    return val;
  }

}
