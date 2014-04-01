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
package org.apache.hadoop.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.mortbay.util.ajax.JSON;

/**
 * This class drives the creation of a mini-cluster on the local machine. By
 * default, a MiniDFSCluster and MiniMRCluster are spawned on the first
 * available ports that are found.
 *
 * A series of command line flags controls the startup cluster options.
 *
 * This class can dump a Hadoop configuration and some basic metadata (in JSON)
 * into a textfile.
 *
 * To shutdown the cluster, kill the process.
 */
public class MiniHadoopClusterManager {
  private static final Log LOG = 
    LogFactory.getLog(MiniHadoopClusterManager.class);
  
  private MiniMRCluster mrc;
  private MiniDFSCluster dfs;
  private String writeDetails;
  private int numTaskTrackers;
  private int numDataNodes;
  private int nameNodePort;
  private int jobTrackerPort;
  private int taskTrackerPort;
  private StartupOption dfsOpts;
  private boolean noDFS;
  private boolean noMR;
  private boolean useLoopBackHosts;
  private String fs;
  private String writeConfig;
  private JobConf conf;

  /**
   * Creates configuration options object.
   */
  @SuppressWarnings("static-access")
  private Options makeOptions() {
    Options options = new Options();
    options
        .addOption("nodfs", false, "Don't start a mini DFS cluster")
        .addOption("nomr", false, "Don't start a mini MR cluster")
        .addOption("useloopbackhosts", false, "Set TaskTracker hostnames to 127.0.1.N")
        .addOption("tasktrackers", true,
            "How many tasktrackers to start (default 1)")
        .addOption("datanodes", true, "How many datanodes to start (default 1)")
        .addOption("format", false, "Format the DFS (default false)")
        .addOption("cmdport", true,
            "Which port to listen on for commands (default 0--we choose)")
        .addOption("nnport", true, "NameNode port (default 0--we choose)")
        .addOption("namenode", true, "URL of the namenode (default "
            + "is either the DFS cluster or a temporary dir)")
        .addOption(
            "tasktrackerport",
            true,
            "Port number to start the tasktracker on (default 0--we choose)")
        .addOption(
            "jobtrackerport",
            true,
            "JobTracker port (default 0--we choose)")
        .addOption(OptionBuilder
            .hasArgs()
            .withArgName("property=value")
            .withDescription("Options to pass into configuration object")
            .create("D"))
        .addOption(OptionBuilder
            .hasArg()
            .withArgName("path")
            .withDescription("Save configuration to this XML file.")
            .create("writeConfig"))
         .addOption(OptionBuilder
            .hasArg()
            .withArgName("path")
            .withDescription("Write basic information to this JSON file.")
            .create("writeDetails"))
        .addOption(OptionBuilder.withDescription("Prints option help.")
            .create("help"));
    return options;
  }

  /**
   * Main entry-point.
   */
  public void run(String[] args) throws IOException {
    if (!parseArguments(args)) {
      return;
    }
    start();
    sleepForever();
  }

  private void sleepForever() {
    while (true) {
      try {
        Thread.sleep(1000*60);
      } catch (InterruptedException _) {
        // nothing
      }
    }
  }

  /**
   * Starts DFS and MR clusters, as specified in member-variable
   * options.  Also writes out configuration and details, if requested.
   * 
   * @throws IOException
   * @throws FileNotFoundException
   */
  public void start() throws IOException, FileNotFoundException {
    if (!noDFS) {
      dfs = new MiniDFSCluster(nameNodePort, conf, numDataNodes,
          true, true, dfsOpts, null, null);
      LOG.info("Started MiniDFSCluster -- namenode on port "
          + dfs.getNameNodePort());
    }
    if (!noMR) {
      if (fs == null && dfs != null) {
        fs = dfs.getFileSystem().getUri().toString();
      } else if (fs == null) {
        fs = "file:///tmp/minimr-" + System.nanoTime();
      }

      String hosts[] = null;
      if (useLoopBackHosts) {
        hosts = new String[numTaskTrackers];
        for (int i = 0; i < numTaskTrackers; i++) {
          hosts[i] = "127.0.1." + (i + 1);
        }
      }

      mrc = new MiniMRCluster(
        jobTrackerPort, taskTrackerPort, numTaskTrackers,
        fs, 1 /* numDir */, null /* racks */, hosts, null /*ugi*/,
        conf);
      LOG.info("Started MiniMRCluster  -- jobtracker on port "
          + mrc.getJobTrackerPort());
    }

    if (writeConfig != null) {
      FileOutputStream fos = new FileOutputStream(new File(writeConfig));
      conf.writeXml(fos);
      fos.close();
    }

    if (writeDetails != null) {
      Map<String, Object> map = new TreeMap<String, Object>();
      if (dfs != null) {
        map.put("namenode_port", dfs.getNameNodePort());
      }
      if (mrc != null) {
        map.put("jobtracker_port", mrc.getJobTrackerPort());
      }
      FileWriter fw = new FileWriter(new File(writeDetails));
      fw.write(new JSON().toJSON(map));
      fw.close();
    }
  }
  
  /**
   * Shuts down in-process clusters.
   */
  public void stop() {
    if (mrc != null) {
      mrc.shutdown();
    }
    if (dfs != null) {
      dfs.shutdown();
    }
  }

  /**
   * Parses arguments and fills out the member variables.
   * @param args Command-line arguments.
   * @return true on successful parse; false to indicate that the 
   * program should exit.
   */
  private boolean parseArguments(String[] args) {
    Options options = makeOptions();
    CommandLine cli;
    try {
      CommandLineParser parser = new GnuParser();
      cli = parser.parse(options, args);
    } catch(ParseException e) {
      LOG.warn("options parsing failed:  "+e.getMessage());
      new HelpFormatter().printHelp("...", options);
      return false;
    }

    if (cli.hasOption("help")) {
      new HelpFormatter().printHelp("...", options);
      return false;
    }
    if (cli.getArgs().length > 0) {
      for (String arg : cli.getArgs()) {
        System.err.println("Unrecognized option: " + arg);
        new HelpFormatter().printHelp("...", options);
        return false;
      }
    }

    // MR
    noMR = cli.hasOption("nomr");
    numTaskTrackers = intArgument(cli, "tasktrackers", 1);
    jobTrackerPort = intArgument(cli, "jobtrackerport", 0);
    taskTrackerPort = intArgument(cli, "tasktrackerport", 0);
    fs = cli.getOptionValue("namenode");

    useLoopBackHosts = cli.hasOption("useloopbackhosts");

    // HDFS
    noDFS = cli.hasOption("nodfs");
    numDataNodes = intArgument(cli, "datanodes", 1);
    nameNodePort = intArgument(cli, "nnport", 0);
    dfsOpts = cli.hasOption("format") ?
        StartupOption.FORMAT : StartupOption.REGULAR;

    // Runner
    writeDetails = cli.getOptionValue("writeDetails");
    writeConfig = cli.getOptionValue("writeConfig");

    // General
    conf = new JobConf();
    updateConfiguration(conf, cli.getOptionValues("D"));
    
    return true;
  }

  /**
   * Updates configuration based on what's given on the command line.
   *
   * @param conf The configuration object
   * @param keyvalues An array of interleaved key value pairs.
   */
  private void updateConfiguration(JobConf conf, String[] keyvalues) {
    int num_confs_updated = 0;
    if (keyvalues != null) {
      for (String prop : keyvalues) {
        String[] keyval = prop.split("=", 2);
        if (keyval.length == 2) {
          conf.set(keyval[0], keyval[1]);
          num_confs_updated++;
        } else {
          LOG.warn("Ignoring -D option " + prop);
        }
      }
    }
    LOG.info("Updated " + num_confs_updated +
        " configuration settings from command line.");
  }

  /**
   * Extracts an integer argument with specified default value.
   */
  private int intArgument(CommandLine cli, String argName, int default_) {
    String o = cli.getOptionValue(argName);
    if (o == null) {
      return default_;
    } else {
      return Integer.parseInt(o);
    }
  }

  /**
   * Starts a MiniHadoopCluster.
   */
  public static void main(String[] args) throws IOException {
    new MiniHadoopClusterManager().run(args);
  }
}
