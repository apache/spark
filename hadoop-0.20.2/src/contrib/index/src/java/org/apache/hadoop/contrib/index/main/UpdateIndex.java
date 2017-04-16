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

package org.apache.hadoop.contrib.index.main;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.index.mapred.IndexUpdateConfiguration;
import org.apache.hadoop.contrib.index.mapred.IIndexUpdater;
import org.apache.hadoop.contrib.index.mapred.Shard;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A distributed "index" is partitioned into "shards". Each shard corresponds
 * to a Lucene instance. This class contains the main() method which uses a
 * Map/Reduce job to analyze documents and update Lucene instances in parallel.
 * 
 * The main() method in UpdateIndex requires the following information for
 * updating the shards:
 *   - Input formatter. This specifies how to format the input documents.
 *   - Analysis. This defines the analyzer to use on the input. The analyzer
 *     determines whether a document is being inserted, updated, or deleted.
 *     For inserts or updates, the analyzer also converts each input document
 *     into a Lucene document.
 *   - Input paths. This provides the location(s) of updated documents,
 *     e.g., HDFS files or directories, or HBase tables.
 *   - Shard paths, or index path with the number of shards. Either specify
 *     the path for each shard, or specify an index path and the shards are
 *     the sub-directories of the index directory.
 *   - Output path. When the update to a shard is done, a message is put here.
 *   - Number of map tasks.
 *
 * All of the information can be specified in a configuration file. All but
 * the first two can also be specified as command line options. Check out
 * conf/index-config.xml.template for other configurable parameters.
 *
 * Note: Because of the parallel nature of Map/Reduce, the behaviour of
 * multiple inserts, deletes or updates to the same document is undefined.
 */
public class UpdateIndex {
  public static final Log LOG = LogFactory.getLog(UpdateIndex.class);

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  private static long now() {
    return System.currentTimeMillis();
  }

  private static void printUsage(String cmd) {
    System.err.println("Usage: java " + UpdateIndex.class.getName() + "\n"
        + "                        -inputPaths <inputPath,inputPath>\n"
        + "                        -outputPath <outputPath>\n"
        + "                        -shards     <shardDir,shardDir>\n"
        + "                        -indexPath  <indexPath>\n"
        + "                        -numShards  <num>\n"
        + "                        -numMapTasks <num>\n"
        + "                        -conf       <confPath>\n"
        + "Note: Do not use both -shards option and -indexPath option.");
  }

  private static String getIndexPath(Configuration conf) {
    return conf.get("sea.index.path");
  }

  private static int getNumShards(Configuration conf) {
    return conf.getInt("sea.num.shards", 1);
  }

  private static Shard[] createShards(String indexPath, int numShards,
      Configuration conf) throws IOException {

    String parent = Shard.normalizePath(indexPath) + Path.SEPARATOR;
    long versionNumber = -1;
    long generation = -1;

    FileSystem fs = FileSystem.get(conf);
    Path path = new Path(indexPath);

    if (fs.exists(path)) {
      FileStatus[] fileStatus = fs.listStatus(path);
      String[] shardNames = new String[fileStatus.length];
      int count = 0;
      for (int i = 0; i < fileStatus.length; i++) {
        if (fileStatus[i].isDir()) {
          shardNames[count] = fileStatus[i].getPath().getName();
          count++;
        }
      }
      Arrays.sort(shardNames, 0, count);

      Shard[] shards = new Shard[count >= numShards ? count : numShards];
      for (int i = 0; i < count; i++) {
        shards[i] =
            new Shard(versionNumber, parent + shardNames[i], generation);
      }

      int number = count;
      for (int i = count; i < numShards; i++) {
        String shardPath;
        while (true) {
          shardPath = parent + NUMBER_FORMAT.format(number++);
          if (!fs.exists(new Path(shardPath))) {
            break;
          }
        }
        shards[i] = new Shard(versionNumber, shardPath, generation);
      }
      return shards;
    } else {
      Shard[] shards = new Shard[numShards];
      for (int i = 0; i < shards.length; i++) {
        shards[i] =
            new Shard(versionNumber, parent + NUMBER_FORMAT.format(i),
                generation);
      }
      return shards;
    }
  }

  /**
   * The main() method
   * @param argv
   */
  public static void main(String[] argv) {
    if (argv.length == 0) {
      printUsage("");
      System.exit(-1);
    }

    String inputPathsString = null;
    Path outputPath = null;
    String shardsString = null;
    String indexPath = null;
    int numShards = -1;
    int numMapTasks = -1;
    Configuration conf = new Configuration();
    String confPath = null;

    // parse the command line
    for (int i = 0; i < argv.length; i++) { // parse command line
      if (argv[i].equals("-inputPaths")) {
        inputPathsString = argv[++i];
      } else if (argv[i].equals("-outputPath")) {
        outputPath = new Path(argv[++i]);
      } else if (argv[i].equals("-shards")) {
        shardsString = argv[++i];
      } else if (argv[i].equals("-indexPath")) {
        indexPath = argv[++i];
      } else if (argv[i].equals("-numShards")) {
        numShards = Integer.parseInt(argv[++i]);
      } else if (argv[i].equals("-numMapTasks")) {
        numMapTasks = Integer.parseInt(argv[++i]);
      } else if (argv[i].equals("-conf")) {
        // add as a local FS resource
        confPath = argv[++i];
        conf.addResource(new Path(confPath));
      } else {
        System.out.println("Unknown option " + argv[i] + " w/ value "
            + argv[++i]);
      }
    }
    LOG.info("inputPaths = " + inputPathsString);
    LOG.info("outputPath = " + outputPath);
    LOG.info("shards     = " + shardsString);
    LOG.info("indexPath  = " + indexPath);
    LOG.info("numShards  = " + numShards);
    LOG.info("numMapTasks= " + numMapTasks);
    LOG.info("confPath   = " + confPath);

    Path[] inputPaths = null;
    Shard[] shards = null;

    JobConf jobConf = new JobConf(conf);
    IndexUpdateConfiguration iconf = new IndexUpdateConfiguration(jobConf);

    if (inputPathsString != null) {
      jobConf.set("mapred.input.dir", inputPathsString);
    }
    inputPaths = FileInputFormat.getInputPaths(jobConf);
    if (inputPaths.length == 0) {
      inputPaths = null;
    }

    if (outputPath == null) {
      outputPath = FileOutputFormat.getOutputPath(jobConf);
    }

    if (inputPaths == null || outputPath == null) {
      System.err.println("InputPaths and outputPath must be specified.");
      printUsage("");
      System.exit(-1);
    }

    if (shardsString != null) {
      iconf.setIndexShards(shardsString);
    }
    shards = Shard.getIndexShards(iconf);
    if (shards != null && shards.length == 0) {
      shards = null;
    }

    if (indexPath == null) {
      indexPath = getIndexPath(conf);
    }
    if (numShards <= 0) {
      numShards = getNumShards(conf);
    }

    if (shards == null && indexPath == null) {
      System.err.println("Either shards or indexPath must be specified.");
      printUsage("");
      System.exit(-1);
    }

    if (numMapTasks <= 0) {
      numMapTasks = jobConf.getNumMapTasks();
    }

    try {
      // create shards and set their directories if necessary
      if (shards == null) {
        shards = createShards(indexPath, numShards, conf);
      }

      long startTime = now();
      try {
        IIndexUpdater updater =
            (IIndexUpdater) ReflectionUtils.newInstance(
                iconf.getIndexUpdaterClass(), conf);
        LOG.info("sea.index.updater = "
            + iconf.getIndexUpdaterClass().getName());

        updater.run(conf, inputPaths, outputPath, numMapTasks, shards);
        LOG.info("Index update job is done");

      } finally {
        long elapsedTime = now() - startTime;
        LOG.info("Elapsed time is  " + (elapsedTime / 1000) + "s");
        System.out.println("Elapsed time is " + (elapsedTime / 1000) + "s");
      }
    } catch (Exception e) {
      e.printStackTrace(System.err);
    }
  }
}
