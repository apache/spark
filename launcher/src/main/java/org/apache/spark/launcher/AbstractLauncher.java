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

package org.apache.spark.launcher;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

/**
 * Basic functionality for launchers.
 */
public abstract class AbstractLauncher<T extends AbstractLauncher> extends LauncherCommon {

  protected static final String DEFAULT_MEM = "512m";

  protected String javaHome;
  protected String sparkHome;
  protected String propertiesFile;
  protected final Map<String, String> conf = new HashMap<String, String>();
  private final Map<String, String> env;

  protected AbstractLauncher() {
    this(null);
  }

  protected AbstractLauncher(Map<String, String> env) {
    this.env = env;
  }

  @SuppressWarnings("unchecked")
  private final T THIS = (T) this;

  /** Set a custom JAVA_HOME for launching the Spark application. */
  public T setJavaHome(String path) {
    checkNotNull(path, "path");
    this.javaHome = path;
    return THIS;
  }

  /** Set a custom Spark installation location for the application. */
  public T setSparkHome(String path) {
    checkNotNull(path, "path");
    this.sparkHome = path;
    return THIS;
  }

  /** Set a custom properties file with Spark configuration for the application. */
  public T setPropertiesFile(String path) {
    checkNotNull(path, "path");
    this.propertiesFile = path;
    return THIS;
  }

  /** Set a single configuration value for the application. */
  public T setConf(String key, String value) {
    checkNotNull(key, "key");
    checkNotNull(value, "value");
    checkArgument(key.startsWith("spark."), "'key' must start with 'spark.'");
    conf.put(key, value);
    return THIS;
  }

  /**
   * Launchers should implement this to create the command to be executed.
   */
  protected abstract List<String> buildLauncherCommand() throws IOException;

  protected Properties loadPropertiesFile() throws IOException {
    Properties props = new Properties();
    File propsFile;
    if (propertiesFile != null) {
      propsFile = new File(propertiesFile);
      checkArgument(propsFile.isFile(), "Invalid properties file '%s'.", propertiesFile);
    } else {
      String confDir = getenv("SPARK_CONF_DIR");
      if (confDir == null) {
        confDir = join(File.separator, getSparkHome(), "conf");
      }
      propsFile = new File(confDir, "spark-defaults.conf");
    }

    if (propsFile.isFile()) {
      FileInputStream fd = null;
      try {
        fd = new FileInputStream(propsFile);
        props.load(new InputStreamReader(fd, "UTF-8"));
      } finally {
        if (fd != null) {
          try {
            fd.close();
          } catch (IOException e) {
            // Ignore.
          }
        }
      }
    }

    return props;
  }

  protected String getSparkHome() {
    String path = first(sparkHome, getenv("SPARK_HOME"));
    checkState(path != null,
        "Spark home not found; set it explicitly or use the SPARK_HOME environment variable.");
    return path;
  }

  protected List<String> createJavaCommand() throws IOException {
    List<String> cmd = new ArrayList<String>();
    if (javaHome == null) {
      cmd.add(join(File.separator, System.getProperty("java.home"), "..", "bin", "java"));
    } else {
      cmd.add(join(File.separator, javaHome, "bin", "java"));
    }

    // Don't set MaxPermSize for Java 8 and later.
    String[] version = System.getProperty("java.version").split("\\.");
    if (Integer.parseInt(version[0]) == 1 && Integer.parseInt(version[1]) < 8) {
      cmd.add("-XX:MaxPermSize=128m");
    }

    // Load extra JAVA_OPTS from conf/java-opts, if it exists.
    File javaOpts = new File(join(File.separator, getSparkHome(), "conf", "java-opts"));
    if (javaOpts.isFile()) {
      BufferedReader br = new BufferedReader(new InputStreamReader(
          new FileInputStream(javaOpts), "UTF-8"));
      try {
        String line;
        while ((line = br.readLine()) != null) {
          addOptionString(cmd, line);
        }
      } finally {
        br.close();
      }
    }

    return cmd;
  }

  protected void addOptionString(List<String> cmd, String options) {
    if (!isEmpty(options)) {
      for (String opt : parseOptionString(options)) {
        cmd.add(opt);
      }
    }
  }

  /**
   * Builds the classpath for the application. Returns a list with one classpath entry per element;
   * each entry is formatted in the way expected by <i>java.net.URLClassLoader</i> (more
   * specifically, with trailing slashes for directories).
   */
  protected List<String> buildClassPath(String appClassPath) throws IOException {
    String sparkHome = getSparkHome();
    String scala = getScalaVersion();

    List<String> cp = new ArrayList<String>();
    addToClassPath(cp, getenv("SPARK_CLASSPATH"));
    addToClassPath(cp, appClassPath);

    String confDir = getenv("SPARK_CONF_DIR");
    if (!isEmpty(confDir)) {
      addToClassPath(cp, confDir);
    } else {
      addToClassPath(cp, join(File.separator, getSparkHome(), "conf"));
    }

    boolean prependClasses = !isEmpty(getenv("SPARK_PREPEND_CLASSES"));
    boolean isTesting = "1".equals(getenv("SPARK_TESTING"));
    if (prependClasses || isTesting) {
      List<String> projects = Arrays.asList("core", "repl", "mllib", "bagel", "graphx",
        "streaming", "tools", "sql/catalyst", "sql/core", "sql/hive", "sql/hive-thriftserver",
        "yarn", "launcher");
      if (prependClasses) {
        System.err.println(
          "NOTE: SPARK_PREPEND_CLASSES is set, placing locally compiled Spark classes ahead of " +
          "assembly.");
        for (String project : projects) {
          addToClassPath(cp, String.format("%s/%s/target/scala-%s/classes", sparkHome, project,
            scala));
        }
      }
      if (isTesting) {
        for (String project : projects) {
          addToClassPath(cp, String.format("%s/%s/target/scala-%s/test-classes", sparkHome,
            project, scala));
        }
      }
      addToClassPath(cp, String.format("%s/core/target/jars/*", sparkHome));
    }

    String assembly = findAssembly(scala);
    addToClassPath(cp, assembly);

    // When Hive support is needed, Datanucleus jars must be included on the classpath. Datanucleus
    // jars do not work if only included in the uber jar as plugin.xml metadata is lost. Both sbt
    // and maven will populate "lib_managed/jars/" with the datanucleus jars when Spark is built
    // with Hive, so first check if the datanucleus jars exist, and then ensure the current Spark
    // assembly is built for Hive, before actually populating the CLASSPATH with the jars.
    //
    // This block also serves as a check for SPARK-1703, when the assembly jar is built with
    // Java 7 and ends up with too many files, causing issues with other JDK versions.
    boolean needsDataNucleus = false;
    JarFile assemblyJar = null;
    try {
      assemblyJar = new JarFile(assembly);
      needsDataNucleus = assemblyJar.getEntry("org/apache/hadoop/hive/ql/exec/") != null;
    } catch (IOException ioe) {
      if (ioe.getMessage().indexOf("invalid CEN header") > 0) {
        System.err.println(
          "Loading Spark jar with '$JAR_CMD' failed.\n" +
          "This is likely because Spark was compiled with Java 7 and run\n" +
          "with Java 6 (see SPARK-1703). Please use Java 7 to run Spark\n" +
          "or build Spark with Java 6.");
        System.exit(1);
      } else {
        throw ioe;
      }
    } finally {
      if (assemblyJar != null) {
        try {
          assemblyJar.close();
        } catch (IOException e) {
          // Ignore.
        }
      }
    }

    if (needsDataNucleus) {
      System.err.println("Spark assembly has been built with Hive, including Datanucleus jars " +
        "in classpath.");
      File libdir;
      if (new File(sparkHome, "RELEASE").isFile()) {
        libdir = new File(sparkHome, "lib");
      } else {
        libdir = new File(sparkHome, "lib_managed/jars");
      }

      checkState(libdir.isDirectory(), "Library directory '%s' does not exist.",
        libdir.getAbsolutePath());
      for (File jar : libdir.listFiles()) {
        if (jar.getName().startsWith("datanucleus-")) {
          addToClassPath(cp, jar.getAbsolutePath());
        }
      }
    }

    addToClassPath(cp, getenv("HADOOP_CONF_DIR"));
    addToClassPath(cp, getenv("YARN_CONF_DIR"));
    addToClassPath(cp, getenv("SPARK_DIST_CLASSPATH"));
    return cp;
  }

  private void addToClassPath(List<String> cp, String entries) {
    if (isEmpty(entries)) {
      return;
    }
    String[] split = entries.split(Pattern.quote(File.pathSeparator));
    for (String entry : split) {
      if (!isEmpty(entry)) {
        if (new File(entry).isDirectory() && !entry.endsWith(File.separator)) {
          entry += File.separator;
        }
        cp.add(entry);
      }
    }
  }

  protected String getScalaVersion() {
    String scala = getenv("SPARK_SCALA_VERSION");
    if (scala != null) {
      return scala;
    }

    String sparkHome = getSparkHome();
    File scala210 = new File(sparkHome, "assembly/target/scala-2.10");
    File scala211 = new File(sparkHome, "assembly/target/scala-2.11");
    if (scala210.isDirectory() && scala211.isDirectory()) {
      checkState(false,
        "Presence of build for both scala versions (2.10 and 2.11) detected.\n" +
        "Either clean one of them or set SPARK_SCALA_VERSION in your environment.");
    } else if (scala210.isDirectory()) {
      return "2.10";
    } else {
      checkState(scala211.isDirectory(), "Cannot find any assembly build directories.");
      return "2.11";
    }

    throw new IllegalStateException("Should not reach here.");
  }

  /**
   * Which OS is running defines two things:
   * - the name of the environment variable used to define the lookup path for native libs
   * - how to execute the command in general.
   *
   * The name is easy: PATH on Win32, DYLD_LIBRARY_PATH on MacOS, LD_LIBRARY_PATH elsewhere.
   *
   * On Unix-like, we're assuming bash is available. So we print one argument per line to
   * the output, and use bash's array handling to execute the right thing.
   *
    * For Win32, see {@link #prepareForWindows(List<String>,String)}.
   */
  protected List<String> prepareForOs(List<String> cmd,
      String libPath,
      Map<String, String> env) {

    // If SPARK_HOME does not come from the environment, explicitly set it
    // in the child's environment.
    Map<String, String> childEnv = env;
    if (System.getenv("SPARK_HOME") == null && !env.containsKey("SPARK_HOME")) {
      childEnv = new HashMap<String, String>(env);
      childEnv.put("SPARK_HOME", sparkHome);
    }

    if (isWindows()) {
      return prepareForWindows(cmd, libPath, childEnv);
    }

    if (isEmpty(libPath) && childEnv.isEmpty()) {
      return cmd;
    }

    List<String> newCmd = new ArrayList<String>();
    newCmd.add("env");

    if (!isEmpty(libPath)) {
      String envName = getLibPathEnvName();
      String currEnvValue = getenv(envName);
      String newEnvValue = join(File.pathSeparator, currEnvValue, libPath);
      newCmd.add(String.format("%s=%s", envName, newEnvValue));
    }
    for (Map.Entry<String, String> e : childEnv.entrySet()) {
      newCmd.add(String.format("%s=%s", e.getKey(), e.getValue()));
    }
    newCmd.addAll(cmd);
    return newCmd;
  }

  protected String shQuote(String s) {
    StringBuilder quoted = new StringBuilder();
    boolean hasWhitespace = false;
    for (int i = 0; i < s.length(); i++) {
      if (Character.isWhitespace(s.codePointAt(i))) {
        quoted.append('"');
        hasWhitespace = true;
        break;
      }
    }

    for (int i = 0; i < s.length(); i++) {
      int cp = s.codePointAt(i);
      switch (cp) {
        case '\'':
          if (hasWhitespace) {
            quoted.appendCodePoint(cp);
            break;
          }
        case '"':
        case '\\':
          quoted.append('\\');
          // Fall through.
        default:
          if (Character.isWhitespace(cp)) {
            hasWhitespace=true;
          }
          quoted.appendCodePoint(cp);
      }
    }
    if (hasWhitespace) {
      quoted.append('"');
    }
    return quoted.toString();
  }

  // Visible for testing.
  List<String> parseOptionString(String s) {
    List<String> opts = new ArrayList<String>();
    StringBuilder opt = new StringBuilder();
    boolean inOpt = false;
    boolean inSingleQuote = false;
    boolean inDoubleQuote = false;
    boolean escapeNext = false;
    boolean hasData = false;

    for (int i = 0; i < s.length(); i++) {
      int c = s.codePointAt(i);
      if (escapeNext) {
        if (!inOpt) {
          inOpt = true;
        }
        opt.appendCodePoint(c);
        escapeNext = false;
      } else if (inOpt) {
        switch (c) {
        case '\\':
          if (inSingleQuote) {
            opt.appendCodePoint(c);
          } else {
            escapeNext = true;
          }
          break;
        case '\'':
          if (inDoubleQuote) {
            opt.appendCodePoint(c);
          } else {
            inSingleQuote = !inSingleQuote;
          }
          break;
        case '"':
          if (inSingleQuote) {
            opt.appendCodePoint(c);
          } else {
            inDoubleQuote = !inDoubleQuote;
          }
          break;
        default:
          if (inSingleQuote || inDoubleQuote || !Character.isWhitespace(c)) {
            opt.appendCodePoint(c);
          } else {
            finishOpt(opts, opt);
            inOpt = false;
            hasData = false;
          }
        }
      } else {
        switch (c) {
        case '\'':
          inSingleQuote = true;
          inOpt = true;
          hasData = true;
          break;
        case '"':
          inDoubleQuote = true;
          inOpt = true;
          hasData = true;
          break;
        case '\\':
          escapeNext = true;
          break;
        default:
          if (!Character.isWhitespace(c)) {
            inOpt = true;
            opt.appendCodePoint(c);
          }
        }
      }
    }

    checkArgument(!inSingleQuote && !inDoubleQuote && !escapeNext, "Invalid option string: %s", s);
    if (opt.length() > 0 || hasData) {
      opts.add(opt.toString());
    }
    return opts;
  }

  private void finishOpt(List<String> opts, StringBuilder opt) {
    opts.add(opt.toString());
    opt.setLength(0);
  }

  private String findAssembly(String scalaVersion) {
    String sparkHome = getSparkHome();
    File libdir;
    if (new File(sparkHome, "RELEASE").isFile()) {
      libdir = new File(sparkHome, "lib");
      checkState(libdir.isDirectory(), "Library directory '%s' does not exist.",
          libdir.getAbsolutePath());
    } else {
      libdir = new File(sparkHome, String.format("assembly/target/scala-%s", scalaVersion));
    }

    final Pattern re = Pattern.compile("spark-assembly.*hadoop.*\\.jar");
    FileFilter filter = new FileFilter() {
      @Override
      public boolean accept(File file) {
        return file.isFile() && re.matcher(file.getName()).matches();
      }
    };
    File[] assemblies = libdir.listFiles(filter);
    checkState(assemblies != null && assemblies.length > 0, "No assemblies found in '%s'.", libdir);
    checkState(assemblies.length == 1, "Multiple assemblies found in '%s'.", libdir);
    return assemblies[0].getAbsolutePath();
  }

  private String getenv(String key) {
    return first(env != null ? env.get(key) : null, System.getenv(key));
  }

  /**
   * Prepare a command line for execution on Windows.
   *
   * Two things need to be done:
   *
   * - If a custom library path is needed, extend PATH to add it. Based on:
   *   http://superuser.com/questions/223104/setting-environment-variable-for-just-one-command-in-windows-cmd-exe
   *
   * - Quote all arguments so that spaces are handled as expected. Quotes within arguments are
   *   "double quoted" (which is batch for escaping a quote). This page has more details about
   *   quoting and other batch script fun stuff: http://ss64.com/nt/syntax-esc.html
   */
  private List<String> prepareForWindows(List<String> cmd,
      String libPath,
      Map<String, String> env) {
    StringBuilder cmdline = new StringBuilder("cmd /c \"");
    if (libPath != null) {
      cmdline.append("set PATH=%PATH%;").append(libPath).append(" &&");
    }
    for (Map.Entry<String, String> e : env.entrySet()) {
      cmdline.append(String.format("set %s=%s", e.getKey(), e.getValue()));
      cmdline.append(" &&");
    }
    for (String arg : cmd) {
      if (cmdline.length() > 0) {
        cmdline.append(" ");
      }
      cmdline.append(quote(arg));
    }
    cmdline.append("\"");
    return Arrays.asList(cmdline.toString());
  }

  /**
   * Quoting arguments that don't need quoting in Windows seems to cause weird issues. So only
   * quote arguments when there is whitespace in them.
   */
  private boolean needsQuoting(String arg) {
    for (int i = 0; i < arg.length(); i++) {
      if (Character.isWhitespace(arg.codePointAt(i))) {
        return true;
      }
    }
    return false;
  }

  private String quote(String arg) {
    if (!needsQuoting(arg)) {
      return arg;
    }
    StringBuilder quoted = new StringBuilder();
    quoted.append("\"");
    for (int i = 0; i < arg.length(); i++) {
      int cp = arg.codePointAt(i);
      if (cp == '\"') {
        quoted.append("\"");
      }
      quoted.appendCodePoint(cp);
    }
    quoted.append("\"");
    return quoted.toString();
  }

  // Visible for testing.
  String getLibPathEnvName() {
    if (isWindows()) {
      return "PATH";
    }

    String os = System.getProperty("os.name");
    if (os.startsWith("Mac OS X")) {
      return "DYLD_LIBRARY_PATH";
    } else {
      return "LD_LIBRARY_PATH";
    }
  }

  protected boolean isWindows() {
    String os = System.getProperty("os.name");
    return os.startsWith("Windows");
  }

}
