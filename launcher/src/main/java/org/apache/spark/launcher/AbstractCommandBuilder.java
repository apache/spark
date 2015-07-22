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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarFile;
import java.util.regex.Pattern;

import static org.apache.spark.launcher.CommandBuilderUtils.*;

/**
 * Abstract Spark command builder that defines common functionality.
 */
abstract class AbstractCommandBuilder {

  boolean verbose;
  String appName;
  String appResource;
  String deployMode;
  String javaHome;
  String mainClass;
  String master;
  String propertiesFile;
  final List<String> appArgs;
  final List<String> jars;
  final List<String> files;
  final List<String> pyFiles;
  final Map<String, String> childEnv;
  final Map<String, String> conf;

  public AbstractCommandBuilder() {
    this.appArgs = new ArrayList<String>();
    this.childEnv = new HashMap<String, String>();
    this.conf = new HashMap<String, String>();
    this.files = new ArrayList<String>();
    this.jars = new ArrayList<String>();
    this.pyFiles = new ArrayList<String>();
  }

  /**
   * Builds the command to execute.
   *
   * @param env A map containing environment variables for the child process. It may already contain
   *            entries defined by the user (such as SPARK_HOME, or those defined by the
   *            SparkLauncher constructor that takes an environment), and may be modified to
   *            include other variables needed by the process to be executed.
   */
  abstract List<String> buildCommand(Map<String, String> env) throws IOException;

  /**
   * Builds a list of arguments to run java.
   *
   * This method finds the java executable to use and appends JVM-specific options for running a
   * class with Spark in the classpath. It also loads options from the "java-opts" file in the
   * configuration directory being used.
   *
   * Callers should still add at least the class to run, as well as any arguments to pass to the
   * class.
   */
  List<String> buildJavaCommand(String extraClassPath) throws IOException {
    List<String> cmd = new ArrayList<String>();
    String envJavaHome;

    if (javaHome != null) {
      cmd.add(join(File.separator, javaHome, "bin", "java"));
    } else if ((envJavaHome = System.getenv("JAVA_HOME")) != null) {
        cmd.add(join(File.separator, envJavaHome, "bin", "java"));
    } else {
        cmd.add(join(File.separator, System.getProperty("java.home"), "bin", "java"));
    }

    // Load extra JAVA_OPTS from conf/java-opts, if it exists.
    File javaOpts = new File(join(File.separator, getConfDir(), "java-opts"));
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

    cmd.add("-cp");
    cmd.add(join(File.pathSeparator, buildClassPath(extraClassPath)));
    return cmd;
  }

  /**
   * Adds the default perm gen size option for Spark if the VM requires it and the user hasn't
   * set it.
   */
  void addPermGenSizeOpt(List<String> cmd) {
    // Don't set MaxPermSize for IBM Java, or Oracle Java 8 and later.
    if (getJavaVendor() == JavaVendor.IBM) {
      return;
    }
    String[] version = System.getProperty("java.version").split("\\.");
    if (Integer.parseInt(version[0]) > 1 || Integer.parseInt(version[1]) > 7) {
      return;
    }

    for (String arg : cmd) {
      if (arg.startsWith("-XX:MaxPermSize=")) {
        return;
      }
    }

    cmd.add("-XX:MaxPermSize=256m");
  }

  void addOptionString(List<String> cmd, String options) {
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
  List<String> buildClassPath(String appClassPath) throws IOException {
    String sparkHome = getSparkHome();

    List<String> cp = new ArrayList<String>();
    addToClassPath(cp, getenv("SPARK_CLASSPATH"));
    addToClassPath(cp, appClassPath);

    addToClassPath(cp, getConfDir());

    boolean prependClasses = !isEmpty(getenv("SPARK_PREPEND_CLASSES"));
    boolean isTesting = "1".equals(getenv("SPARK_TESTING"));
    if (prependClasses || isTesting) {
      String scala = getScalaVersion();
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

      // Add this path to include jars that are shaded in the final deliverable created during
      // the maven build. These jars are copied to this directory during the build.
      addToClassPath(cp, String.format("%s/core/target/jars/*", sparkHome));
    }

    // We can't rely on the ENV_SPARK_ASSEMBLY variable to be set. Certain situations, such as
    // when running unit tests, or user code that embeds Spark and creates a SparkContext
    // with a local or local-cluster master, will cause this code to be called from an
    // environment where that env variable is not guaranteed to exist.
    //
    // For the testing case, we rely on the test code to set and propagate the test classpath
    // appropriately.
    //
    // For the user code case, we fall back to looking for the Spark assembly under SPARK_HOME.
    // That duplicates some of the code in the shell scripts that look for the assembly, though.
    String assembly = getenv(ENV_SPARK_ASSEMBLY);
    if (assembly == null && isEmpty(getenv("SPARK_TESTING"))) {
      assembly = findAssembly();
    }
    addToClassPath(cp, assembly);

    // Datanucleus jars must be included on the classpath. Datanucleus jars do not work if only
    // included in the uber jar as plugin.xml metadata is lost. Both sbt and maven will populate
    // "lib_managed/jars/" with the datanucleus jars when Spark is built with Hive
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

    addToClassPath(cp, getenv("HADOOP_CONF_DIR"));
    addToClassPath(cp, getenv("YARN_CONF_DIR"));
    addToClassPath(cp, getenv("SPARK_DIST_CLASSPATH"));
    return cp;
  }

  /**
   * Adds entries to the classpath.
   *
   * @param cp List to which the new entries are appended.
   * @param entries New classpath entries (separated by File.pathSeparator).
   */
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

  String getScalaVersion() {
    String scala = getenv("SPARK_SCALA_VERSION");
    if (scala != null) {
      return scala;
    }
    String sparkHome = getSparkHome();
    File scala210 = new File(sparkHome, "assembly/target/scala-2.10");
    File scala211 = new File(sparkHome, "assembly/target/scala-2.11");
    checkState(!scala210.isDirectory() || !scala211.isDirectory(),
      "Presence of build for both scala versions (2.10 and 2.11) detected.\n" +
      "Either clean one of them or set SPARK_SCALA_VERSION in your environment.");
    if (scala210.isDirectory()) {
      return "2.10";
    } else {
      checkState(scala211.isDirectory(), "Cannot find any assembly build directories.");
      return "2.11";
    }
  }

  String getSparkHome() {
    String path = getenv(ENV_SPARK_HOME);
    checkState(path != null,
      "Spark home not found; set it explicitly or use the SPARK_HOME environment variable.");
    return path;
  }

  /**
   * Loads the configuration file for the application, if it exists. This is either the
   * user-specified properties file, or the spark-defaults.conf file under the Spark configuration
   * directory.
   */
  Properties loadPropertiesFile() throws IOException {
    Properties props = new Properties();
    File propsFile;
    if (propertiesFile != null) {
      propsFile = new File(propertiesFile);
      checkArgument(propsFile.isFile(), "Invalid properties file '%s'.", propertiesFile);
    } else {
      propsFile = new File(getConfDir(), DEFAULT_PROPERTIES_FILE);
    }

    if (propsFile.isFile()) {
      FileInputStream fd = null;
      try {
        fd = new FileInputStream(propsFile);
        props.load(new InputStreamReader(fd, "UTF-8"));
        for (Map.Entry<Object, Object> e : props.entrySet()) {
          e.setValue(e.getValue().toString().trim());
        }
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

  String getenv(String key) {
    return firstNonEmpty(childEnv.get(key), System.getenv(key));
  }

  private String findAssembly() {
    String sparkHome = getSparkHome();
    File libdir;
    if (new File(sparkHome, "RELEASE").isFile()) {
      libdir = new File(sparkHome, "lib");
      checkState(libdir.isDirectory(), "Library directory '%s' does not exist.",
          libdir.getAbsolutePath());
    } else {
      libdir = new File(sparkHome, String.format("assembly/target/scala-%s", getScalaVersion()));
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

  private String getConfDir() {
    String confDir = getenv("SPARK_CONF_DIR");
    return confDir != null ? confDir : join(File.separator, getSparkHome(), "conf");
  }

}
