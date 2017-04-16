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

package org.apache.hadoop.mapred.pipes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * The main entry point and job submitter. It may either be used as a command
 * line-based or API-based method to launch Pipes jobs.
 */
public class Submitter extends Configured implements Tool {

  protected static final Log LOG = LogFactory.getLog(Submitter.class);
  
  public Submitter() {
    this(new Configuration());
  }
  
  public Submitter(Configuration conf) {
    setConf(conf);
  }
  
  /**
   * Get the URI of the application's executable.
   * @param conf
   * @return the URI where the application's executable is located
   */
  public static String getExecutable(JobConf conf) {
    return conf.get("hadoop.pipes.executable");
  }
  
  /**
   * Set the URI for the application's executable. Normally this is a hdfs: 
   * location.
   * @param conf
   * @param executable The URI of the application's executable.
   */
  public static void setExecutable(JobConf conf, String executable) {
    conf.set("hadoop.pipes.executable", executable);
  }

  /**
   * Set whether the job is using a Java RecordReader.
   * @param conf the configuration to modify
   * @param value the new value
   */
  public static void setIsJavaRecordReader(JobConf conf, boolean value) {
    conf.setBoolean("hadoop.pipes.java.recordreader", value);
  }

  /**
   * Check whether the job is using a Java RecordReader
   * @param conf the configuration to check
   * @return is it a Java RecordReader?
   */
  public static boolean getIsJavaRecordReader(JobConf conf) {
    return conf.getBoolean("hadoop.pipes.java.recordreader", false);
  }

  /**
   * Set whether the Mapper is written in Java.
   * @param conf the configuration to modify
   * @param value the new value
   */
  public static void setIsJavaMapper(JobConf conf, boolean value) {
    conf.setBoolean("hadoop.pipes.java.mapper", value);
  }

  /**
   * Check whether the job is using a Java Mapper.
   * @param conf the configuration to check
   * @return is it a Java Mapper?
   */
  public static boolean getIsJavaMapper(JobConf conf) {
    return conf.getBoolean("hadoop.pipes.java.mapper", false);
  }

  /**
   * Set whether the Reducer is written in Java.
   * @param conf the configuration to modify
   * @param value the new value
   */
  public static void setIsJavaReducer(JobConf conf, boolean value) {
    conf.setBoolean("hadoop.pipes.java.reducer", value);
  }

  /**
   * Check whether the job is using a Java Reducer.
   * @param conf the configuration to check
   * @return is it a Java Reducer?
   */
  public static boolean getIsJavaReducer(JobConf conf) {
    return conf.getBoolean("hadoop.pipes.java.reducer", false);
  }

  /**
   * Set whether the job will use a Java RecordWriter.
   * @param conf the configuration to modify
   * @param value the new value to set
   */
  public static void setIsJavaRecordWriter(JobConf conf, boolean value) {
    conf.setBoolean("hadoop.pipes.java.recordwriter", value);
  }

  /**
   * Will the reduce use a Java RecordWriter?
   * @param conf the configuration to check
   * @return true, if the output of the job will be written by Java
   */
  public static boolean getIsJavaRecordWriter(JobConf conf) {
    return conf.getBoolean("hadoop.pipes.java.recordwriter", false);
  }

  /**
   * Set the configuration, if it doesn't already have a value for the given
   * key.
   * @param conf the configuration to modify
   * @param key the key to set
   * @param value the new "default" value to set
   */
  private static void setIfUnset(JobConf conf, String key, String value) {
    if (conf.get(key) == null) {
      conf.set(key, value);
    }
  }

  /**
   * Save away the user's original partitioner before we override it.
   * @param conf the configuration to modify
   * @param cls the user's partitioner class
   */
  static void setJavaPartitioner(JobConf conf, Class cls) {
    conf.set("hadoop.pipes.partitioner", cls.getName());
  }
  
  /**
   * Get the user's original partitioner.
   * @param conf the configuration to look in
   * @return the class that the user submitted
   */
  static Class<? extends Partitioner> getJavaPartitioner(JobConf conf) {
    return conf.getClass("hadoop.pipes.partitioner", 
                         HashPartitioner.class,
                         Partitioner.class);
  }

  /**
   * Does the user want to keep the command file for debugging? If this is
   * true, pipes will write a copy of the command data to a file in the
   * task directory named "downlink.data", which may be used to run the C++
   * program under the debugger. You probably also want to set 
   * JobConf.setKeepFailedTaskFiles(true) to keep the entire directory from
   * being deleted.
   * To run using the data file, set the environment variable 
   * "hadoop.pipes.command.file" to point to the file.
   * @param conf the configuration to check
   * @return will the framework save the command file?
   */
  public static boolean getKeepCommandFile(JobConf conf) {
    return conf.getBoolean("hadoop.pipes.command-file.keep", false);
  }

  /**
   * Set whether to keep the command file for debugging
   * @param conf the configuration to modify
   * @param keep the new value
   */
  public static void setKeepCommandFile(JobConf conf, boolean keep) {
    conf.setBoolean("hadoop.pipes.command-file.keep", keep);
  }

  /**
   * Submit a job to the map/reduce cluster. All of the necessary modifications
   * to the job to run under pipes are made to the configuration.
   * @param conf the job to submit to the cluster (MODIFIED)
   * @throws IOException
   * @deprecated Use {@link Submitter#runJob(JobConf)}
   */
  @Deprecated
  public static RunningJob submitJob(JobConf conf) throws IOException {
    return runJob(conf);
  }

  /**
   * Submit a job to the map/reduce cluster. All of the necessary modifications
   * to the job to run under pipes are made to the configuration.
   * @param conf the job to submit to the cluster (MODIFIED)
   * @throws IOException
   */
  public static RunningJob runJob(JobConf conf) throws IOException {
    setupPipesJob(conf);
    return JobClient.runJob(conf);
  }

  /**
   * Submit a job to the Map-Reduce framework.
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   * 
   * @param conf the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the
   *         running-job.
   * @throws IOException
   */
  public static RunningJob jobSubmit(JobConf conf) throws IOException {
    setupPipesJob(conf);
    return new JobClient(conf).submitJob(conf);
  }
  
  private static void setupPipesJob(JobConf conf) throws IOException {
    // default map output types to Text
    if (!getIsJavaMapper(conf)) {
      conf.setMapRunnerClass(PipesMapRunner.class);
      // Save the user's partitioner and hook in our's.
      setJavaPartitioner(conf, conf.getPartitionerClass());
      conf.setPartitionerClass(PipesPartitioner.class);
    }
    if (!getIsJavaReducer(conf)) {
      conf.setReducerClass(PipesReducer.class);
      if (!getIsJavaRecordWriter(conf)) {
        conf.setOutputFormat(NullOutputFormat.class);
      }
    }
    String textClassname = Text.class.getName();
    setIfUnset(conf, "mapred.mapoutput.key.class", textClassname);
    setIfUnset(conf, "mapred.mapoutput.value.class", textClassname);
    setIfUnset(conf, "mapred.output.key.class", textClassname);
    setIfUnset(conf, "mapred.output.value.class", textClassname);
    
    // Use PipesNonJavaInputFormat if necessary to handle progress reporting
    // from C++ RecordReaders ...
    if (!getIsJavaRecordReader(conf) && !getIsJavaMapper(conf)) {
      conf.setClass("mapred.pipes.user.inputformat", 
                    conf.getInputFormat().getClass(), InputFormat.class);
      conf.setInputFormat(PipesNonJavaInputFormat.class);
    }
    
    String exec = getExecutable(conf);
    if (exec == null) {
      throw new IllegalArgumentException("No application program defined.");
    }
    // add default debug script only when executable is expressed as
    // <path>#<executable>
    if (exec.contains("#")) {
      DistributedCache.createSymlink(conf);
      // set default gdb commands for map and reduce task 
      String defScript = "$HADOOP_HOME/src/c++/pipes/debug/pipes-default-script";
      setIfUnset(conf,"mapred.map.task.debug.script",defScript);
      setIfUnset(conf,"mapred.reduce.task.debug.script",defScript);
    }
    URI[] fileCache = DistributedCache.getCacheFiles(conf);
    if (fileCache == null) {
      fileCache = new URI[1];
    } else {
      URI[] tmp = new URI[fileCache.length+1];
      System.arraycopy(fileCache, 0, tmp, 1, fileCache.length);
      fileCache = tmp;
    }
    try {
      fileCache[0] = new URI(exec);
    } catch (URISyntaxException e) {
      IOException ie = new IOException("Problem parsing execable URI " + exec);
      ie.initCause(e);
      throw ie;
    }
    DistributedCache.setCacheFiles(fileCache, conf);
  }

  /**
   * A command line parser for the CLI-based Pipes job submitter.
   */
  static class CommandLineParser {
    private Options options = new Options();
    
    void addOption(String longName, boolean required, String description, 
                   String paramName) {
      Option option = OptionBuilder.withArgName(paramName).hasArgs(1).withDescription(description).isRequired(required).create(longName);
      options.addOption(option);
    }
    
    void addArgument(String name, boolean required, String description) {
      Option option = OptionBuilder.withArgName(name).hasArgs(1).withDescription(description).isRequired(required).create();
      options.addOption(option);

    }

    Parser createParser() {
      Parser result = new BasicParser();
      return result;
    }
    
    void printUsage() {
      // The CLI package should do this for us, but I can't figure out how
      // to make it print something reasonable.
      System.out.println("bin/hadoop pipes");
      System.out.println("  [-input <path>] // Input directory");
      System.out.println("  [-output <path>] // Output directory");
      System.out.println("  [-jar <jar file> // jar filename");
      System.out.println("  [-inputformat <class>] // InputFormat class");
      System.out.println("  [-map <class>] // Java Map class");
      System.out.println("  [-partitioner <class>] // Java Partitioner");
      System.out.println("  [-reduce <class>] // Java Reduce class");
      System.out.println("  [-writer <class>] // Java RecordWriter");
      System.out.println("  [-program <executable>] // executable URI");
      System.out.println("  [-reduces <num>] // number of reduces");
      System.out.println();
      GenericOptionsParser.printGenericCommandUsage(System.out);
    }
  }
  
  private static <InterfaceType> 
  Class<? extends InterfaceType> getClass(CommandLine cl, String key, 
                                          JobConf conf, 
                                          Class<InterfaceType> cls
                                         ) throws ClassNotFoundException {
    return conf.getClassByName((String) cl.getOptionValue(key)).asSubclass(cls);
  }

  @Override
  public int run(String[] args) throws Exception {
    CommandLineParser cli = new CommandLineParser();
    if (args.length == 0) {
      cli.printUsage();
      return 1;
    }
    cli.addOption("input", false, "input path to the maps", "path");
    cli.addOption("output", false, "output path from the reduces", "path");
    
    cli.addOption("jar", false, "job jar file", "path");
    cli.addOption("inputformat", false, "java classname of InputFormat", 
                  "class");
    //cli.addArgument("javareader", false, "is the RecordReader in Java");
    cli.addOption("map", false, "java classname of Mapper", "class");
    cli.addOption("partitioner", false, "java classname of Partitioner", 
                  "class");
    cli.addOption("reduce", false, "java classname of Reducer", "class");
    cli.addOption("writer", false, "java classname of OutputFormat", "class");
    cli.addOption("program", false, "URI to application executable", "class");
    cli.addOption("reduces", false, "number of reduces", "num");
    cli.addOption("jobconf", false, 
        "\"n1=v1,n2=v2,..\" (Deprecated) Optional. Add or override a JobConf property.",
        "key=val");
    Parser parser = cli.createParser();
    try {
      
      GenericOptionsParser genericParser = new GenericOptionsParser(getConf(), args);
      CommandLine results = 
        parser.parse(cli.options, genericParser.getRemainingArgs());
      
      JobConf job = new JobConf(getConf());
      
      if (results.hasOption("input")) {
        FileInputFormat.setInputPaths(job, 
                          (String) results.getOptionValue("input"));
      }
      if (results.hasOption("output")) {
        FileOutputFormat.setOutputPath(job, 
          new Path((String) results.getOptionValue("output")));
      }
      if (results.hasOption("jar")) {
        job.setJar((String) results.getOptionValue("jar"));
      }
      if (results.hasOption("inputformat")) {
        setIsJavaRecordReader(job, true);
        job.setInputFormat(getClass(results, "inputformat", job,
                                     InputFormat.class));
      }
      if (results.hasOption("javareader")) {
        setIsJavaRecordReader(job, true);
      }
      if (results.hasOption("map")) {
        setIsJavaMapper(job, true);
        job.setMapperClass(getClass(results, "map", job, Mapper.class));
      }
      if (results.hasOption("partitioner")) {
        job.setPartitionerClass(getClass(results, "partitioner", job,
                                          Partitioner.class));
      }
      if (results.hasOption("reduce")) {
        setIsJavaReducer(job, true);
        job.setReducerClass(getClass(results, "reduce", job, Reducer.class));
      }
      if (results.hasOption("reduces")) {
        job.setNumReduceTasks(Integer.parseInt((String) 
                                            results.getOptionValue("reduces")));
      }
      if (results.hasOption("writer")) {
        setIsJavaRecordWriter(job, true);
        job.setOutputFormat(getClass(results, "writer", job, 
                                      OutputFormat.class));
      }
      if (results.hasOption("program")) {
        setExecutable(job, (String) results.getOptionValue("program"));
      }
      if (results.hasOption("jobconf")) {
        LOG.warn("-jobconf option is deprecated, please use -D instead.");
        String options = (String)results.getOptionValue("jobconf");
        StringTokenizer tokenizer = new StringTokenizer(options, ",");
        while (tokenizer.hasMoreTokens()) {
          String keyVal = tokenizer.nextToken().trim();
          String[] keyValSplit = keyVal.split("=", 2);
          job.set(keyValSplit[0], keyValSplit[1]);
        }
      }
      // if they gave us a jar file, include it into the class path
      String jarFile = job.getJar();
      if (jarFile != null) {
        final URL[] urls = new URL[]{ FileSystem.getLocal(job).
            pathToFile(new Path(jarFile)).toURL()};
        //FindBugs complains that creating a URLClassLoader should be
        //in a doPrivileged() block. 
        ClassLoader loader =
          AccessController.doPrivileged(
              new PrivilegedAction<ClassLoader>() {
                public ClassLoader run() {
                  return new URLClassLoader(urls);
                }
              }
            );
        job.setClassLoader(loader);
      }
      
      runJob(job);
      return 0;
    } catch (ParseException pe) {
      LOG.info("Error : " + pe);
      cli.printUsage();
      return 1;
    }
    
  }
  
  /**
   * Submit a pipes job based on the command line arguments.
   * @param args
   */
  public static void main(String[] args) throws Exception {
    int exitCode =  new Submitter().run(args);
    System.exit(exitCode);
  }

}
