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
package org.apache.hadoop.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <code>GenericOptionsParser</code> is a utility to parse command line
 * arguments generic to the Hadoop framework. 
 * 
 * <code>GenericOptionsParser</code> recognizes several standarad command 
 * line arguments, enabling applications to easily specify a namenode, a 
 * jobtracker, additional configuration resources etc.
 * 
 * <h4 id="GenericOptions">Generic Options</h4>
 * 
 * <p>The supported generic options are:</p>
 * <p><blockquote><pre>
 *     -conf &lt;configuration file&gt;     specify a configuration file
 *     -D &lt;property=value&gt;            use value for given property
 *     -fs &lt;local|namenode:port&gt;      specify a namenode
 *     -jt &lt;local|jobtracker:port&gt;    specify a job tracker
 *     -files &lt;comma separated list of files&gt;    specify comma separated
 *                            files to be copied to the map reduce cluster
 *     -libjars &lt;comma separated list of jars&gt;   specify comma separated
 *                            jar files to include in the classpath.
 *     -archives &lt;comma separated list of archives&gt;    specify comma
 *             separated archives to be unarchived on the compute machines.

 * </pre></blockquote></p>
 * 
 * <p>The general command line syntax is:</p>
 * <p><tt><pre>
 * bin/hadoop command [genericOptions] [commandOptions]
 * </pre></tt></p>
 * 
 * <p>Generic command line arguments <strong>might</strong> modify 
 * <code>Configuration </code> objects, given to constructors.</p>
 * 
 * <p>The functionality is implemented using Commons CLI.</p>
 *
 * <p>Examples:</p>
 * <p><blockquote><pre>
 * $ bin/hadoop dfs -fs darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 * 
 * $ bin/hadoop dfs -D fs.default.name=darwin:8020 -ls /data
 * list /data directory in dfs with namenode darwin:8020
 *     
 * $ bin/hadoop dfs -conf hadoop-site.xml -ls /data
 * list /data directory in dfs with conf specified in hadoop-site.xml
 *     
 * $ bin/hadoop job -D mapred.job.tracker=darwin:50020 -submit job.xml
 * submit a job to job tracker darwin:50020
 *     
 * $ bin/hadoop job -jt darwin:50020 -submit job.xml
 * submit a job to job tracker darwin:50020
 *     
 * $ bin/hadoop job -jt local -submit job.xml
 * submit a job to local runner
 * 
 * $ bin/hadoop jar -libjars testlib.jar 
 * -archives test.tgz -files file.txt inputjar args
 * job submission with libjars, files and archives
 * </pre></blockquote></p>
 *
 * @see Tool
 * @see ToolRunner
 */
public class GenericOptionsParser {

  private static final Log LOG = LogFactory.getLog(GenericOptionsParser.class);
  private Configuration conf;
  private CommandLine commandLine;

  /**
   * Create an options parser with the given options to parse the args.
   * @param opts the options
   * @param args the command line arguments
   * @throws IOException 
   */
  public GenericOptionsParser(Options opts, String[] args) 
      throws IOException {
    this(new Configuration(), new Options(), args);
  }

  /**
   * Create an options parser to parse the args.
   * @param args the command line arguments
   * @throws IOException 
   */
  public GenericOptionsParser(String[] args) 
      throws IOException {
    this(new Configuration(), new Options(), args);
  }
  
  /** 
   * Create a <code>GenericOptionsParser<code> to parse only the generic Hadoop  
   * arguments. 
   * 
   * The array of string arguments other than the generic arguments can be 
   * obtained by {@link #getRemainingArgs()}.
   * 
   * @param conf the <code>Configuration</code> to modify.
   * @param args command-line arguments.
   * @throws IOException 
   */
  public GenericOptionsParser(Configuration conf, String[] args) 
      throws IOException {
    this(conf, new Options(), args); 
  }

  /** 
   * Create a <code>GenericOptionsParser</code> to parse given options as well 
   * as generic Hadoop options. 
   * 
   * The resulting <code>CommandLine</code> object can be obtained by 
   * {@link #getCommandLine()}.
   * 
   * @param conf the configuration to modify  
   * @param options options built by the caller 
   * @param args User-specified arguments
   * @throws IOException 
   */
  public GenericOptionsParser(Configuration conf,
      Options options, String[] args) throws IOException {
    parseGeneralOptions(options, conf, args);
    this.conf = conf;
  }

  /**
   * Returns an array of Strings containing only application-specific arguments.
   * 
   * @return array of <code>String</code>s containing the un-parsed arguments
   * or <strong>empty array</strong> if commandLine was not defined.
   */
  public String[] getRemainingArgs() {
    return (commandLine == null) ? new String[]{} : commandLine.getArgs();
  }

  /**
   * Get the modified configuration
   * @return the configuration that has the modified parameters.
   */
  public Configuration getConfiguration() {
    return conf;
  }

  /**
   * Returns the commons-cli <code>CommandLine</code> object 
   * to process the parsed arguments. 
   * 
   * Note: If the object is created with 
   * {@link #GenericOptionsParser(Configuration, String[])}, then returned 
   * object will only contain parsed generic options.
   * 
   * @return <code>CommandLine</code> representing list of arguments 
   *         parsed against Options descriptor.
   */
  public CommandLine getCommandLine() {
    return commandLine;
  }

  /**
   * Specify properties of each generic option
   */
  @SuppressWarnings("static-access")
  private static Options buildGeneralOptions(Options opts) {
    Option fs = OptionBuilder.withArgName("local|namenode:port")
    .hasArg()
    .withDescription("specify a namenode")
    .create("fs");
    Option jt = OptionBuilder.withArgName("local|jobtracker:port")
    .hasArg()
    .withDescription("specify a job tracker")
    .create("jt");
    Option oconf = OptionBuilder.withArgName("configuration file")
    .hasArg()
    .withDescription("specify an application configuration file")
    .create("conf");
    Option property = OptionBuilder.withArgName("property=value")
    .hasArg()
    .withDescription("use value for given property")
    .create('D');
    Option libjars = OptionBuilder.withArgName("paths")
    .hasArg()
    .withDescription("comma separated jar files to include in the classpath.")
    .create("libjars");
    Option files = OptionBuilder.withArgName("paths")
    .hasArg()
    .withDescription("comma separated files to be copied to the " +
           "map reduce cluster")
    .create("files");
    Option archives = OptionBuilder.withArgName("paths")
    .hasArg()
    .withDescription("comma separated archives to be unarchived" +
                     " on the compute machines.")
    .create("archives");
    // file with security tokens
    Option tokensFile = OptionBuilder.withArgName("tokensFile")
    .hasArg()
    .withDescription("name of the file with the tokens")
    .create("tokenCacheFile");

    opts.addOption(fs);
    opts.addOption(jt);
    opts.addOption(oconf);
    opts.addOption(property);
    opts.addOption(libjars);
    opts.addOption(files);
    opts.addOption(archives);
    opts.addOption(tokensFile);

    return opts;
  }

  /**
   * Modify configuration according user-specified generic options
   * @param conf Configuration to be modified
   * @param line User-specified generic options
   */
  private void processGeneralOptions(Configuration conf,
      CommandLine line) throws IOException {
    if (line.hasOption("fs")) {
      FileSystem.setDefaultUri(conf, line.getOptionValue("fs"));
    }

    if (line.hasOption("jt")) {
      conf.set("mapred.job.tracker", line.getOptionValue("jt"));
    }
    if (line.hasOption("conf")) {
      String[] values = line.getOptionValues("conf");
      for(String value : values) {
        conf.addResource(new Path(value));
      }
    }
    if (line.hasOption("libjars")) {
      conf.set("tmpjars", 
               validateFiles(line.getOptionValue("libjars"), conf));
      //setting libjars in client classpath
      URL[] libjars = getLibJars(conf);
      if(libjars!=null && libjars.length>0) {
        conf.setClassLoader(new URLClassLoader(libjars, conf.getClassLoader()));
        Thread.currentThread().setContextClassLoader(
            new URLClassLoader(libjars, 
                Thread.currentThread().getContextClassLoader()));
      }
    }
    if (line.hasOption("files")) {
      conf.set("tmpfiles", 
               validateFiles(line.getOptionValue("files"), conf));
    }
    if (line.hasOption("archives")) {
      conf.set("tmparchives", 
                validateFiles(line.getOptionValue("archives"), conf));
    }
    if (line.hasOption('D')) {
      String[] property = line.getOptionValues('D');
      for(String prop : property) {
        String[] keyval = prop.split("=", 2);
        if (keyval.length == 2) {
          conf.set(keyval[0], keyval[1]);
        }
      }
    }
    conf.setBoolean("mapred.used.genericoptionsparser", true);
    
    // tokensFile
    if(line.hasOption("tokenCacheFile")) {
      String fileName = line.getOptionValue("tokenCacheFile");
      // check if the local file exists
      try 
      {
        FileSystem localFs = FileSystem.getLocal(conf);
        Path p = new Path(fileName);
        if (!localFs.exists(p)) {
          throw new FileNotFoundException("File "+fileName+" does not exist.");
        }

        LOG.debug("setting conf tokensFile: " + fileName);
        conf.set("mapreduce.job.credentials.json", 
                 localFs.makeQualified(p).toString());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  /**
   * If libjars are set in the conf, parse the libjars.
   * @param conf
   * @return libjar urls
   * @throws IOException
   */
  public static URL[] getLibJars(Configuration conf) throws IOException {
    String jars = conf.get("tmpjars");
    if(jars==null) {
      return null;
    }
    String[] files = jars.split(",");
    List<URL> cp = new ArrayList<URL>();
    for (String file : files) {
      Path tmp = new Path(file);
      if (tmp.getFileSystem(conf).equals(FileSystem.getLocal(conf))) {
        cp.add(FileSystem.getLocal(conf).pathToFile(tmp).toURI().toURL());
      }
    }
    return cp.toArray(new URL[0]);
  }

  /**
   * takes input as a comma separated list of files
   * and verifies if they exist. It defaults for file:///
   * if the files specified do not have a scheme.
   * it returns the paths uri converted defaulting to file:///.
   * So an input of  /home/user/file1,/home/user/file2 would return
   * file:///home/user/file1,file:///home/user/file2
   * @param files
   * @return
   */
  private String validateFiles(String files, Configuration conf) 
      throws IOException  {
    if (files == null) 
      return null;
    String[] fileArr = files.split(",");
    String[] finalArr = new String[fileArr.length];
    for (int i =0; i < fileArr.length; i++) {
      String tmp = fileArr[i];
      String finalPath;
      URI pathURI;
      try {
        pathURI = new URI(tmp);
      } catch (URISyntaxException e) {
        throw new IllegalArgumentException(e);
      }
      Path path = new Path(pathURI);
      FileSystem localFs = FileSystem.getLocal(conf);
      if (pathURI.getScheme() == null) {
        //default to the local file system
        //check if the file exists or not first
        if (!localFs.exists(path)) {
          throw new FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = path.makeQualified(localFs).toString();
      }
      else {
        // check if the file exists in this file system
        // we need to recreate this filesystem object to copy
        // these files to the file system jobtracker is running
        // on.
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
          throw new FileNotFoundException("File " + tmp + " does not exist.");
        }
        finalPath = path.makeQualified(fs).toString();
      }
      finalArr[i] = finalPath;
    }
    return StringUtils.arrayToString(finalArr);
  }
  

  /**
   * Parse the user-specified options, get the generic options, and modify
   * configuration accordingly
   * @param conf Configuration to be modified
   * @param args User-specified arguments
   * @return Command-specific arguments
   */
  private String[] parseGeneralOptions(Options opts, Configuration conf, 
      String[] args) throws IOException {
    opts = buildGeneralOptions(opts);
    CommandLineParser parser = new GnuParser();
    try {
      commandLine = parser.parse(opts, args, true);
      processGeneralOptions(conf, commandLine);
      return commandLine.getArgs();
    } catch(ParseException e) {
      LOG.warn("options parsing failed: "+e.getMessage());

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", opts);
    }
    return args;
  }

  /**
   * Print the usage message for generic command-line options supported.
   * 
   * @param out stream to print the usage message to.
   */
  public static void printGenericCommandUsage(PrintStream out) {
    
    out.println("Generic options supported are");
    out.println("-conf <configuration file>     specify an application configuration file");
    out.println("-D <property=value>            use value for given property");
    out.println("-fs <local|namenode:port>      specify a namenode");
    out.println("-jt <local|jobtracker:port>    specify a job tracker");
    out.println("-files <comma separated list of files>    " + 
      "specify comma separated files to be copied to the map reduce cluster");
    out.println("-libjars <comma separated list of jars>    " +
      "specify comma separated jar files to include in the classpath.");
    out.println("-archives <comma separated list of archives>    " +
                "specify comma separated archives to be unarchived" +
                " on the compute machines.\n");
    out.println("The general command line syntax is");
    out.println("bin/hadoop command [genericOptions] [commandOptions]\n");
  }
  
}
