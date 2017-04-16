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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Parser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileAsTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorCombiner;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregatorReducer;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.streaming.io.IdentifierResolver;
import org.apache.hadoop.streaming.io.InputWriter;
import org.apache.hadoop.streaming.io.OutputReader;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;

/** All the client-side work happens here.
 * (Jar packaging, MapRed job submission and monitoring)
 */
public class StreamJob implements Tool {

  protected static final Log LOG = LogFactory.getLog(StreamJob.class.getName());
  final static String REDUCE_NONE = "NONE";
    
  /** -----------Streaming CLI Implementation  **/
  private CommandLineParser parser = new BasicParser(); 
  private Options allOptions;
  /**@deprecated use StreamJob() with ToolRunner or set the 
   * Configuration using {@link #setConf(Configuration)} and 
   * run with {@link #run(String[])}.  
   */
  @Deprecated
  public StreamJob(String[] argv, boolean mayExit) {
    this();
    argv_ = argv;
    this.config_ = new Configuration();
  }
  
  public StreamJob() {
    setupOptions();
    this.config_ = new Configuration();
  }
  
  @Override
  public Configuration getConf() {
    return config_;
  }

  @Override
  public void setConf(Configuration conf) {
    this.config_ = conf;
  }
  
  @Override
  public int run(String[] args) throws Exception {
    try {
      this.argv_ = args;
      init();
  
      preProcessArgs();
      parseArgv();
      postProcessArgs();
  
      setJobConf();
      return submitAndMonitorJob();
    }catch (IllegalArgumentException ex) {
      //ignore, since log will already be printed
      // print the log in debug mode.
      LOG.debug("Error in streaming job", ex);
      return 1;
    }
  }
  
  /**
   * This method creates a streaming job from the given argument list.
   * The created object can be used and/or submitted to a jobtracker for 
   * execution by a job agent such as JobControl
   * @param argv the list args for creating a streaming job
   * @return the created JobConf object 
   * @throws IOException
   */
  static public JobConf createJob(String[] argv) throws IOException {
    StreamJob job = new StreamJob();
    job.argv_ = argv;
    job.init();
    job.preProcessArgs();
    job.parseArgv();
    job.postProcessArgs();
    job.setJobConf();
    return job.jobConf_;
  }

  /**
   * This is the method that actually 
   * intializes the job conf and submits the job
   * to the jobtracker
   * @throws IOException
   * @deprecated use {@link #run(String[])} instead.
   */
  @Deprecated
  public int go() throws IOException {
    try {
      return run(argv_);
    }
    catch (Exception ex) {
      throw new IOException(ex.getMessage());
    }
  }
  
  protected void init() {
    try {
      env_ = new Environment();
    } catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  void preProcessArgs() {
    verbose_ = false;
    addTaskEnvironment_ = "";
  }

  void postProcessArgs() throws IOException {
    
    if (inputSpecs_.size() == 0) {
      fail("Required argument: -input <name>");
    }
    if (output_ == null) {
      fail("Required argument: -output ");
    }
    msg("addTaskEnvironment=" + addTaskEnvironment_);

    Iterator it = packageFiles_.iterator();
    while (it.hasNext()) {
      File f = new File((String) it.next());
      if (f.isFile()) {
        shippedCanonFiles_.add(f.getCanonicalPath());
      }
    }
    msg("shippedCanonFiles_=" + shippedCanonFiles_);

    // careful with class names..
    mapCmd_ = unqualifyIfLocalPath(mapCmd_);
    comCmd_ = unqualifyIfLocalPath(comCmd_);
    redCmd_ = unqualifyIfLocalPath(redCmd_);
  }

  String unqualifyIfLocalPath(String cmd) throws IOException {
    if (cmd == null) {
      //
    } else {
      String prog = cmd;
      String args = "";
      int s = cmd.indexOf(" ");
      if (s != -1) {
        prog = cmd.substring(0, s);
        args = cmd.substring(s + 1);
      }
      String progCanon;
      try {
        progCanon = new File(prog).getCanonicalPath();
      } catch (IOException io) {
        progCanon = prog;
      }
      boolean shipped = shippedCanonFiles_.contains(progCanon);
      msg("shipped: " + shipped + " " + progCanon);
      if (shipped) {
        // Change path to simple filename.
        // That way when PipeMapRed calls Runtime.exec(),
        // it will look for the excutable in Task's working dir.
        // And this is where TaskRunner unjars our job jar.
        prog = new File(prog).getName();
        if (args.length() > 0) {
          cmd = prog + " " + args;
        } else {
          cmd = prog;
        }
      }
    }
    msg("cmd=" + cmd);
    return cmd;
  }

  void parseArgv(){
    CommandLine cmdLine = null; 
    try{
      cmdLine = parser.parse(allOptions, argv_);
    }catch(Exception oe){
      LOG.error(oe.getMessage());
      exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
    }
    
    if (cmdLine != null){
      verbose_ =  cmdLine.hasOption("verbose");
      detailedUsage_ = cmdLine.hasOption("info");
      debug_ = cmdLine.hasOption("debug")? debug_ + 1 : debug_;
      
      String[] values = cmdLine.getOptionValues("input");
      if (values != null && values.length > 0) {
        for (String input : values) {
          inputSpecs_.add(input);
        }
      }
      output_ = (String) cmdLine.getOptionValue("output");
 
      
      mapCmd_ = (String)cmdLine.getOptionValue("mapper"); 
      comCmd_ = (String)cmdLine.getOptionValue("combiner"); 
      redCmd_ = (String)cmdLine.getOptionValue("reducer"); 
      
      values = cmdLine.getOptionValues("file");
      if (values != null && values.length > 0) {
        StringBuilder unpackRegex = new StringBuilder(
          config_.getPattern(JobContext.JAR_UNPACK_PATTERN,
                             JobConf.UNPACK_JAR_PATTERN_DEFAULT).pattern());
        for (String file : values) {
          packageFiles_.add(file);
          String fname = new File(file).getName();
          unpackRegex.append("|(?:").append(Pattern.quote(fname)).append(")");
        }
        config_.setPattern(JobContext.JAR_UNPACK_PATTERN,
                           Pattern.compile(unpackRegex.toString()));
        validate(packageFiles_);
      }

         
      String fsName = (String)cmdLine.getOptionValue("dfs");
      if (null != fsName){
        LOG.warn("-dfs option is deprecated, please use -fs instead.");
        config_.set("fs.default.name", fsName);
      }
      
      additionalConfSpec_ = (String)cmdLine.getOptionValue("additionalconfspec"); 
      inputFormatSpec_ = (String)cmdLine.getOptionValue("inputformat"); 
      outputFormatSpec_ = (String)cmdLine.getOptionValue("outputformat");
      numReduceTasksSpec_ = (String)cmdLine.getOptionValue("numReduceTasks"); 
      partitionerSpec_ = (String)cmdLine.getOptionValue("partitioner");
      inReaderSpec_ = (String)cmdLine.getOptionValue("inputreader"); 
      mapDebugSpec_ = (String)cmdLine.getOptionValue("mapdebug");    
      reduceDebugSpec_ = (String)cmdLine.getOptionValue("reducedebug");
      ioSpec_ = (String)cmdLine.getOptionValue("io");
      
      String[] car = cmdLine.getOptionValues("cacheArchive");
      if (null != car && car.length > 0){
        LOG.warn("-cacheArchive option is deprecated, please use -archives instead.");
        for(String s : car){
          cacheArchives = (cacheArchives == null)?s :cacheArchives + "," + s;  
        }
      }

      String[] caf = cmdLine.getOptionValues("cacheFile");
      if (null != caf && caf.length > 0){
        LOG.warn("-cacheFile option is deprecated, please use -files instead.");
        for(String s : caf){
          cacheFiles = (cacheFiles == null)?s :cacheFiles + "," + s;  
        }
      }
      
      String[] jobconf = cmdLine.getOptionValues("jobconf");
      if (null != jobconf && jobconf.length > 0){ 
        LOG.warn("-jobconf option is deprecated, please use -D instead.");
        for(String s : jobconf){
          String []parts = s.split("=", 2); 
          config_.set(parts[0], parts[1]);
        }
      }
      
      String[] cmd = cmdLine.getOptionValues("cmdenv");
      if (null != cmd && cmd.length > 0){
        for(String s : cmd) {
          if (addTaskEnvironment_.length() > 0) {
            addTaskEnvironment_ += " ";
          }
          addTaskEnvironment_ += s;
        }
      }
    }else {
      exitUsage(argv_.length > 0 && "-info".equals(argv_[0]));
    }
  }

  protected void msg(String msg) {
    if (verbose_) {
      System.out.println("STREAM: " + msg);
    }
  }
  
  private Option createOption(String name, String desc, 
                              String argName, int max, boolean required){
    return OptionBuilder
           .withArgName(argName)
           .hasArgs(max)
           .withDescription(desc)
           .isRequired(required)
           .create(name);
  }
  
  private Option createBoolOption(String name, String desc){
    return OptionBuilder.withDescription(desc).create(name);
  }
  
  private void validate(final List<String> values) 
  throws IllegalArgumentException {
    for (String file : values) {
      File f = new File(file);  
      if (!f.canRead()) {
        fail("File: " + f.getAbsolutePath() 
          + " does not exist, or is not readable."); 
      }
    }
  }
  
  private void setupOptions(){

    Option input   = createOption("input", 
                                  "DFS input file(s) for the Map step", 
                                  "path", 
                                  Integer.MAX_VALUE, 
                                  true);  
    
    Option output  = createOption("output", 
                                  "DFS output directory for the Reduce step", 
                                  "path", 1, true); 
    Option mapper  = createOption("mapper", 
                                  "The streaming command to run", "cmd", 1, false);
    Option combiner = createOption("combiner", 
                                   "The streaming command to run", "cmd", 1, false);
    // reducer could be NONE 
    Option reducer = createOption("reducer", 
                                  "The streaming command to run", "cmd", 1, false); 
    Option file = createOption("file", 
                               "File to be shipped in the Job jar file", 
                               "file", Integer.MAX_VALUE, false); 
    Option dfs = createOption("dfs", 
                              "Optional. Override DFS configuration", "<h:p>|local", 1, false); 
    Option jt = createOption("jt", 
                             "Optional. Override JobTracker configuration", "<h:p>|local", 1, false);
    Option additionalconfspec = createOption("additionalconfspec", 
                                             "Optional.", "spec", 1, false);
    Option inputformat = createOption("inputformat", 
                                      "Optional.", "spec", 1, false);
    Option outputformat = createOption("outputformat", 
                                       "Optional.", "spec", 1, false);
    Option partitioner = createOption("partitioner", 
                                      "Optional.", "spec", 1, false);
    Option numReduceTasks = createOption("numReduceTasks", 
        "Optional.", "spec",1, false );
    Option inputreader = createOption("inputreader", 
                                      "Optional.", "spec", 1, false);
    Option mapDebug = createOption("mapdebug",
                                   "Optional.", "spec", 1, false);
    Option reduceDebug = createOption("reducedebug",
                                      "Optional", "spec",1, false);
    Option jobconf = 
      createOption("jobconf", 
                   "(n=v) Optional. Add or override a JobConf property.", 
                   "spec", 1, false);
    
    Option cmdenv = 
      createOption("cmdenv", "(n=v) Pass env.var to streaming commands.", 
                   "spec", 1, false);
    Option cacheFile = createOption("cacheFile", 
                                    "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    Option cacheArchive = createOption("cacheArchive", 
                                       "File name URI", "fileNameURI", Integer.MAX_VALUE, false);
    Option io = createOption("io",
                             "Optional.", "spec", 1, false);
    
    // boolean properties
    
    Option verbose = createBoolOption("verbose", "print verbose output"); 
    Option info = createBoolOption("info", "print verbose output"); 
    Option help = createBoolOption("help", "print this help message"); 
    Option debug = createBoolOption("debug", "print debug output"); 
    
    allOptions = new Options().
      addOption(input).
      addOption(output).
      addOption(mapper).
      addOption(combiner).
      addOption(reducer).
      addOption(file).
      addOption(dfs).
      addOption(jt).
      addOption(additionalconfspec).
      addOption(inputformat).
      addOption(outputformat).
      addOption(partitioner).
      addOption(numReduceTasks).
      addOption(inputreader).
      addOption(mapDebug).
      addOption(reduceDebug).
      addOption(jobconf).
      addOption(cmdenv).
      addOption(cacheFile).
      addOption(cacheArchive).
      addOption(io).
      addOption(verbose).
      addOption(info).
      addOption(debug).
      addOption(help);
  }

  public void exitUsage(boolean detailed) {
    //         1         2         3         4         5         6         7
    //1234567890123456789012345678901234567890123456789012345678901234567890123456789
    
    System.out.println("Usage: $HADOOP_HOME/bin/hadoop jar \\");
    System.out.println("          $HADOOP_HOME/hadoop-streaming.jar [options]");
    System.out.println("Options:");
    System.out.println("  -input    <path>     DFS input file(s) for the Map step");
    System.out.println("  -output   <path>     DFS output directory for the Reduce step");
    System.out.println("  -mapper   <cmd|JavaClassName>      The streaming command to run");
    System.out.println("  -combiner <cmd|JavaClassName>" + 
                       " The streaming command to run");
    System.out.println("  -reducer  <cmd|JavaClassName>      The streaming command to run");
    System.out.println("  -file     <file>     File/dir to be shipped in the Job jar file");
    System.out.println("  -inputformat TextInputFormat(default)|SequenceFileAsTextInputFormat|JavaClassName Optional.");
    System.out.println("  -outputformat TextOutputFormat(default)|JavaClassName  Optional.");
    System.out.println("  -partitioner JavaClassName  Optional.");
    System.out.println("  -numReduceTasks <num>  Optional.");
    System.out.println("  -inputreader <spec>  Optional.");
    System.out.println("  -cmdenv   <n>=<v>    Optional. Pass env.var to streaming commands");
    System.out.println("  -mapdebug <path>  Optional. " +
    "To run this script when a map task fails ");
    System.out.println("  -reducedebug <path>  Optional." +
    " To run this script when a reduce task fails ");
    System.out.println("  -io <identifier>  Optional.");
    System.out.println("  -verbose");
    System.out.println();
    GenericOptionsParser.printGenericCommandUsage(System.out);

    if (!detailed) {
      System.out.println();      
      System.out.println("For more details about these options:");
      System.out.println("Use $HADOOP_HOME/bin/hadoop jar build/hadoop-streaming.jar -info");
      fail("");
    }
    System.out.println();
    System.out.println("In -input: globbing on <path> is supported and can have multiple -input");
    System.out.println("Default Map input format: a line is a record in UTF-8");
    System.out.println("  the key part ends at first TAB, the rest of the line is the value");
    System.out.println("Custom input format: -inputformat package.MyInputFormat ");
    System.out.println("Map output format, reduce input/output format:");
    System.out.println("  Format defined by what the mapper command outputs. Line-oriented");
    System.out.println();
    System.out.println("The files named in the -file argument[s] end up in the");
    System.out.println("  working directory when the mapper and reducer are run.");
    System.out.println("  The location of this working directory is unspecified.");
    System.out.println();
    System.out.println("To set the number of reduce tasks (num. of output files):");
    System.out.println("  -D mapred.reduce.tasks=10");
    System.out.println("To skip the sort/combine/shuffle/sort/reduce step:");
    System.out.println("  Use -numReduceTasks 0");
    System.out
      .println("  A Task's Map output then becomes a 'side-effect output' rather than a reduce input");
    System.out
      .println("  This speeds up processing, This also feels more like \"in-place\" processing");
    System.out.println("  because the input filename and the map input order are preserved");
    System.out.println("  This equivalent -reducer NONE");
    System.out.println();
    System.out.println("To speed up the last maps:");
    System.out.println("  -D mapred.map.tasks.speculative.execution=true");
    System.out.println("To speed up the last reduces:");
    System.out.println("  -D mapred.reduce.tasks.speculative.execution=true");
    System.out.println("To name the job (appears in the JobTracker Web UI):");
    System.out.println("  -D mapred.job.name='My Job' ");
    System.out.println("To change the local temp directory:");
    System.out.println("  -D dfs.data.dir=/tmp/dfs");
    System.out.println("  -D stream.tmpdir=/tmp/streaming");
    System.out.println("Additional local temp directories with -cluster local:");
    System.out.println("  -D mapred.local.dir=/tmp/local");
    System.out.println("  -D mapred.system.dir=/tmp/system");
    System.out.println("  -D mapred.temp.dir=/tmp/temp");
    System.out.println("To treat tasks with non-zero exit status as SUCCEDED:");    
    System.out.println("  -D stream.non.zero.exit.is.failure=false");
    System.out.println("Use a custom hadoopStreaming build along a standard hadoop install:");
    System.out.println("  $HADOOP_HOME/bin/hadoop jar /path/my-hadoop-streaming.jar [...]\\");
    System.out
      .println("    [...] -D stream.shipped.hadoopstreaming=/path/my-hadoop-streaming.jar");
    System.out.println("For more details about jobconf parameters see:");
    System.out.println("  http://wiki.apache.org/hadoop/JobConfFile");
    System.out.println("To set an environement variable in a streaming command:");
    System.out.println("   -cmdenv EXAMPLE_DIR=/home/example/dictionaries/");
    System.out.println();
    System.out.println("Shortcut:");
    System.out
      .println("   setenv HSTREAMING \"$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-streaming.jar\"");
    System.out.println();
    System.out.println("Example: $HSTREAMING -mapper \"/usr/local/bin/perl5 filter.pl\"");
    System.out.println("           -file /local/filter.pl -input \"/logs/0604*/*\" [...]");
    System.out.println("  Ships a script, invokes the non-shipped perl interpreter");
    System.out.println("  Shipped files go to the working directory so filter.pl is found by perl");
    System.out.println("  Input files are all the daily logs for days in month 2006-04");
    fail("");
  }

  public void fail(String message) {    
    System.err.println(message);
    throw new IllegalArgumentException(message);
  }

  // --------------------------------------------

  protected String getHadoopClientHome() {
    String h = env_.getProperty("HADOOP_HOME"); // standard Hadoop
    if (h == null) {
      //fail("Missing required environment variable: HADOOP_HOME");
      h = "UNDEF";
    }
    return h;
  }

  protected boolean isLocalHadoop() {
    return StreamUtil.isLocalJobTracker(jobConf_);
  }

  @Deprecated
  protected String getClusterNick() {
    return "default";
  }

  /** @return path to the created Jar file or null if no files are necessary.
   */
  protected String packageJobJar() throws IOException {
    ArrayList unjarFiles = new ArrayList();

    // Runtime code: ship same version of code as self (job submitter code)
    // usually found in: build/contrib or build/hadoop-<version>-dev-streaming.jar

    // First try an explicit spec: it's too hard to find our own location in this case:
    // $HADOOP_HOME/bin/hadoop jar /not/first/on/classpath/custom-hadoop-streaming.jar
    // where findInClasspath() would find the version of hadoop-streaming.jar in $HADOOP_HOME
    String runtimeClasses = config_.get("stream.shipped.hadoopstreaming"); // jar or class dir
    
    if (runtimeClasses == null) {
      runtimeClasses = StreamUtil.findInClasspath(StreamJob.class.getName());
    }
    if (runtimeClasses == null) {
      throw new IOException("runtime classes not found: " + getClass().getPackage());
    } else {
      msg("Found runtime classes in: " + runtimeClasses);
    }
    if (isLocalHadoop()) {
      // don't package class files (they might get unpackaged in "." and then
      //  hide the intended CLASSPATH entry)
      // we still package everything else (so that scripts and executable are found in
      //  Task workdir like distributed Hadoop)
    } else {
      if (new File(runtimeClasses).isDirectory()) {
        packageFiles_.add(runtimeClasses);
      } else {
        unjarFiles.add(runtimeClasses);
      }
    }
    if (packageFiles_.size() + unjarFiles.size() == 0) {
      return null;
    }
    String tmp = jobConf_.get("stream.tmpdir"); //, "/tmp/${user.name}/"
    File tmpDir = (tmp == null) ? null : new File(tmp);
    // tmpDir=null means OS default tmp dir
    File jobJar = File.createTempFile("streamjob", ".jar", tmpDir);
    System.out.println("packageJobJar: " + packageFiles_ + " " + unjarFiles + " " + jobJar
                       + " tmpDir=" + tmpDir);
    if (debug_ == 0) {
      jobJar.deleteOnExit();
    }
    JarBuilder builder = new JarBuilder();
    if (verbose_) {
      builder.setVerbose(true);
    }
    String jobJarName = jobJar.getAbsolutePath();
    builder.merge(packageFiles_, unjarFiles, jobJarName);
    return jobJarName;
  }
  
  /**
   * get the uris of all the files/caches
   */
  protected void getURIs(String lcacheArchives, String lcacheFiles) {
    String archives[] = StringUtils.getStrings(lcacheArchives);
    String files[] = StringUtils.getStrings(lcacheFiles);
    fileURIs = StringUtils.stringToURI(files);
    archiveURIs = StringUtils.stringToURI(archives);
  }
  
  protected void setJobConf() throws IOException {
    if (additionalConfSpec_ != null) {
      LOG.warn("-additionalconfspec option is deprecated, please use -conf instead.");
      config_.addResource(new Path(additionalConfSpec_));
    }

    // general MapRed job properties
    jobConf_ = new JobConf(config_);
    
    // All streaming jobs get the task timeout value
    // from the configuration settings.

    // The correct FS must be set before this is called!
    // (to resolve local vs. dfs drive letter differences) 
    // (mapred.working.dir will be lazily initialized ONCE and depends on FS)
    for (int i = 0; i < inputSpecs_.size(); i++) {
      FileInputFormat.addInputPaths(jobConf_, 
                        (String) inputSpecs_.get(i));
    }
    jobConf_.set("stream.numinputspecs", "" + inputSpecs_.size());

    String defaultPackage = this.getClass().getPackage().getName();
    Class c;
    Class fmt = null;
    if (inReaderSpec_ == null && inputFormatSpec_ == null) {
      fmt = TextInputFormat.class;
    } else if (inputFormatSpec_ != null) {
      if (inputFormatSpec_.equals(TextInputFormat.class.getName())
          || inputFormatSpec_.equals(TextInputFormat.class.getCanonicalName())
          || inputFormatSpec_.equals(TextInputFormat.class.getSimpleName())) {
        fmt = TextInputFormat.class;
      } else if (inputFormatSpec_.equals(KeyValueTextInputFormat.class
          .getName())
          || inputFormatSpec_.equals(KeyValueTextInputFormat.class
              .getCanonicalName())
          || inputFormatSpec_.equals(KeyValueTextInputFormat.class.getSimpleName())) {
      } else if (inputFormatSpec_.equals(SequenceFileInputFormat.class
          .getName())
          || inputFormatSpec_
              .equals(org.apache.hadoop.mapred.SequenceFileInputFormat.class
                  .getCanonicalName())
          || inputFormatSpec_
              .equals(org.apache.hadoop.mapred.SequenceFileInputFormat.class.getSimpleName())) {
      } else if (inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class
          .getName())
          || inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class
              .getCanonicalName())
          || inputFormatSpec_.equals(SequenceFileAsTextInputFormat.class.getSimpleName())) {
        fmt = SequenceFileAsTextInputFormat.class;
      } else {
        c = StreamUtil.goodClassOrNull(jobConf_, inputFormatSpec_, defaultPackage);
        if (c != null) {
          fmt = c;
        } else {
          fail("-inputformat : class not found : " + inputFormatSpec_);
        }
      }
    } 
    if (fmt == null) {
      fmt = StreamInputFormat.class;
    }

    jobConf_.setInputFormat(fmt);

    if (ioSpec_ != null) {
      jobConf_.set("stream.map.input", ioSpec_);
      jobConf_.set("stream.map.output", ioSpec_);
      jobConf_.set("stream.reduce.input", ioSpec_);
      jobConf_.set("stream.reduce.output", ioSpec_);
    }
    
    Class<? extends IdentifierResolver> idResolverClass = 
      jobConf_.getClass("stream.io.identifier.resolver.class",
        IdentifierResolver.class, IdentifierResolver.class);
    IdentifierResolver idResolver = ReflectionUtils.newInstance(idResolverClass, jobConf_);
    
    idResolver.resolve(jobConf_.get("stream.map.input", IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.map.input.writer.class",
      idResolver.getInputWriterClass(), InputWriter.class);
    
    idResolver.resolve(jobConf_.get("stream.reduce.input", IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.reduce.input.writer.class",
      idResolver.getInputWriterClass(), InputWriter.class);
    
    idResolver.resolve(jobConf_.get("stream.map.output", IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.map.output.reader.class",
      idResolver.getOutputReaderClass(), OutputReader.class);
    jobConf_.setMapOutputKeyClass(idResolver.getOutputKeyClass());
    jobConf_.setMapOutputValueClass(idResolver.getOutputValueClass());
    
    idResolver.resolve(jobConf_.get("stream.reduce.output", IdentifierResolver.TEXT_ID));
    jobConf_.setClass("stream.reduce.output.reader.class",
      idResolver.getOutputReaderClass(), OutputReader.class);
    jobConf_.setOutputKeyClass(idResolver.getOutputKeyClass());
    jobConf_.setOutputValueClass(idResolver.getOutputValueClass());
    
    jobConf_.set("stream.addenvironment", addTaskEnvironment_);

    if (mapCmd_ != null) {
      c = StreamUtil.goodClassOrNull(jobConf_, mapCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setMapperClass(c);
      } else {
        jobConf_.setMapperClass(PipeMapper.class);
        jobConf_.setMapRunnerClass(PipeMapRunner.class);
        jobConf_.set("stream.map.streamprocessor", 
                     URLEncoder.encode(mapCmd_, "UTF-8"));
      }
    }

    if (comCmd_ != null) {
      c = StreamUtil.goodClassOrNull(jobConf_, comCmd_, defaultPackage);
      if (c != null) {
        jobConf_.setCombinerClass(c);
      } else {
        jobConf_.setCombinerClass(PipeCombiner.class);
        jobConf_.set("stream.combine.streamprocessor", URLEncoder.encode(
                comCmd_, "UTF-8"));
      }
    }

    boolean reducerNone_ = false;
    if (redCmd_ != null) {
      reducerNone_ = redCmd_.equals(REDUCE_NONE);
      if (redCmd_.compareToIgnoreCase("aggregate") == 0) {
        jobConf_.setReducerClass(ValueAggregatorReducer.class);
        jobConf_.setCombinerClass(ValueAggregatorCombiner.class);
      } else {

        c = StreamUtil.goodClassOrNull(jobConf_, redCmd_, defaultPackage);
        if (c != null) {
          jobConf_.setReducerClass(c);
        } else {
          jobConf_.setReducerClass(PipeReducer.class);
          jobConf_.set("stream.reduce.streamprocessor", URLEncoder.encode(
              redCmd_, "UTF-8"));
        }
      }
    }

    if (inReaderSpec_ != null) {
      String[] args = inReaderSpec_.split(",");
      String readerClass = args[0];
      // this argument can only be a Java class
      c = StreamUtil.goodClassOrNull(jobConf_, readerClass, defaultPackage);
      if (c != null) {
        jobConf_.set("stream.recordreader.class", c.getName());
      } else {
        fail("-inputreader: class not found: " + readerClass);
      }
      for (int i = 1; i < args.length; i++) {
        String[] nv = args[i].split("=", 2);
        String k = "stream.recordreader." + nv[0];
        String v = (nv.length > 1) ? nv[1] : "";
        jobConf_.set(k, v);
      }
    }
    
    FileOutputFormat.setOutputPath(jobConf_, new Path(output_));
    fmt = null;
    if (outputFormatSpec_!= null) {
      c = StreamUtil.goodClassOrNull(jobConf_, outputFormatSpec_, defaultPackage);
      if (c != null) {
        fmt = c;
      } else {
        fail("-outputformat : class not found : " + outputFormatSpec_);
      }
    }
    if (fmt == null) {
      fmt = TextOutputFormat.class;
    }
    jobConf_.setOutputFormat(fmt);

    if (partitionerSpec_!= null) {
      c = StreamUtil.goodClassOrNull(jobConf_, partitionerSpec_, defaultPackage);
      if (c != null) {
        jobConf_.setPartitionerClass(c);
      } else {
        fail("-partitioner : class not found : " + partitionerSpec_);
      }
    }
    
    if (numReduceTasksSpec_!= null) {
      int numReduceTasks = Integer.parseInt(numReduceTasksSpec_);
      jobConf_.setNumReduceTasks(numReduceTasks);
    }
    if (reducerNone_) {
      jobConf_.setNumReduceTasks(0);
    }
    
    if(mapDebugSpec_ != null){
    	jobConf_.setMapDebugScript(mapDebugSpec_);
    }
    if(reduceDebugSpec_ != null){
    	jobConf_.setReduceDebugScript(reduceDebugSpec_);
    }
    // last, allow user to override anything
    // (although typically used with properties we didn't touch)

    jar_ = packageJobJar();
    if (jar_ != null) {
      jobConf_.setJar(jar_);
    }
    
    if ((cacheArchives != null) || (cacheFiles != null)){
      getURIs(cacheArchives, cacheFiles);
      boolean b = DistributedCache.checkURIs(fileURIs, archiveURIs);
      if (!b)
        fail(LINK_URI);
    }
    DistributedCache.createSymlink(jobConf_);
    // set the jobconf for the caching parameters
    if (cacheArchives != null)
      DistributedCache.setCacheArchives(archiveURIs, jobConf_);
    if (cacheFiles != null)
      DistributedCache.setCacheFiles(fileURIs, jobConf_);
    
    if (verbose_) {
      listJobConfProperties();
    }
   
    msg("submitting to jobconf: " + getJobTrackerHostPort());
  }

  /**
   * Prints out the jobconf properties on stdout
   * when verbose is specified.
   */
  protected void listJobConfProperties()
  {
    msg("==== JobConf properties:");
    Iterator it = jobConf_.iterator();
    TreeMap sorted = new TreeMap();
    while(it.hasNext()) {
      Map.Entry en = (Map.Entry)it.next();
      sorted.put(en.getKey(), en.getValue());
    }
    it = sorted.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry en = (Map.Entry)it.next();
      msg(en.getKey() + "=" + en.getValue());
    }
    msg("====");
  }

  protected String getJobTrackerHostPort() {
    return jobConf_.get("mapred.job.tracker");
  }

  protected void jobInfo() {
    if (isLocalHadoop()) {
      LOG.info("Job running in-process (local Hadoop)");
    } else {
      String hp = getJobTrackerHostPort();
      LOG.info("To kill this job, run:");
      LOG.info(getHadoopClientHome() + "/bin/hadoop job  -Dmapred.job.tracker=" + hp + " -kill "
               + jobId_);
      //LOG.info("Job file: " + running_.getJobFile());
      LOG.info("Tracking URL: " + StreamUtil.qualifyHost(running_.getTrackingURL()));
    }
  }

  // Based on JobClient
  public int submitAndMonitorJob() throws IOException {

    if (jar_ != null && isLocalHadoop()) {
      // getAbs became required when shell and subvm have different working dirs...
      File wd = new File(".").getAbsoluteFile();
      StreamUtil.unJar(new File(jar_), wd);
    }

    // if jobConf_ changes must recreate a JobClient
    jc_ = new JobClient(jobConf_);
    boolean error = true;
    running_ = null;
    String lastReport = null;
    try {
      running_ = jc_.submitJob(jobConf_);
      jobId_ = running_.getID();

      LOG.info("getLocalDirs(): " + Arrays.asList(jobConf_.getLocalDirs()));
      LOG.info("Running job: " + jobId_);
      jobInfo();

      while (!running_.isComplete()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        running_ = jc_.getJob(jobId_);
        String report = null;
        report = " map " + Math.round(running_.mapProgress() * 100) + "%  reduce "
          + Math.round(running_.reduceProgress() * 100) + "%";

        if (!report.equals(lastReport)) {
          LOG.info(report);
          lastReport = report;
        }
      }
      if (!running_.isSuccessful()) {
        jobInfo();
	LOG.error("Job not successful. Error: " + running_.getFailureInfo());
	return 1;
      }
      LOG.info("Job complete: " + jobId_);
      LOG.info("Output: " + output_);
      error = false;
    } catch(FileNotFoundException fe) {
      LOG.error("Error launching job , bad input path : " + fe.getMessage());
      return 2;
    } catch(InvalidJobConfException je) {
      LOG.error("Error launching job , Invalid job conf : " + je.getMessage());
      return 3;
    } catch(FileAlreadyExistsException fae) {
      LOG.error("Error launching job , Output path already exists : " 
                + fae.getMessage());
      return 4;
    } catch(IOException ioe) {
      LOG.error("Error Launching job : " + ioe.getMessage());
      return 5;
    } finally {
      if (error && (running_ != null)) {
        LOG.info("killJob...");
        running_.killJob();
      }
      jc_.close();
    }
    return 0;
  }

  protected String[] argv_;
  protected boolean verbose_;
  protected boolean detailedUsage_;
  protected int debug_;

  protected Environment env_;

  protected String jar_;
  protected boolean localHadoop_;
  protected Configuration config_;
  protected JobConf jobConf_;
  protected JobClient jc_;

  // command-line arguments
  protected ArrayList inputSpecs_ = new ArrayList(); // <String>
  protected TreeSet seenPrimary_ = new TreeSet(); // <String>
  protected boolean hasSimpleInputSpecs_;
  protected ArrayList packageFiles_ = new ArrayList(); // <String>
  protected ArrayList shippedCanonFiles_ = new ArrayList(); // <String>
  //protected TreeMap<String, String> userJobConfProps_ = new TreeMap<String, String>(); 
  protected String output_;
  protected String mapCmd_;
  protected String comCmd_;
  protected String redCmd_;
  protected String cacheFiles;
  protected String cacheArchives;
  protected URI[] fileURIs;
  protected URI[] archiveURIs;
  protected String inReaderSpec_;
  protected String inputFormatSpec_;
  protected String outputFormatSpec_;
  protected String partitionerSpec_;
  protected String numReduceTasksSpec_;
  protected String additionalConfSpec_;
  protected String mapDebugSpec_;
  protected String reduceDebugSpec_;
  protected String ioSpec_;

  // Use to communicate config to the external processes (ex env.var.HADOOP_USER)
  // encoding "a=b c=d"
  protected String addTaskEnvironment_;

  protected boolean outputSingleNode_;
  protected long minRecWrittenToEnableSkip_;

  protected RunningJob running_;
  protected JobID jobId_;
  protected static final String LINK_URI = "You need to specify the uris as hdfs://host:port/#linkname," +
    "Please specify a different link name for all of your caching URIs";

}
