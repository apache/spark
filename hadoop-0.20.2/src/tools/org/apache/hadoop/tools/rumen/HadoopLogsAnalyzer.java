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
/**
 * 
 */
package org.apache.hadoop.tools.rumen;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.EOFException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.LineReader;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

/**
 * This is the main class for rumen log mining functionality.
 * 
 * It reads a directory of job tracker logs, and computes various information
 * about it. See {@code usage()}, below.
 * 
 */
public class HadoopLogsAnalyzer extends Configured implements Tool {

  // output streams
  private PrintStream statusOutput = System.out;
  private PrintStream statisticalOutput = System.out;

  private static PrintStream staticDebugOutput = System.err;

  /**
   * The number of splits a task can have, before we ignore them all.
   */
  private final static int MAXIMUM_PREFERRED_LOCATIONS = 25;

  /**
   * This element is to compensate for the fact that our percentiles engine
   * rounds up for the expected sample count, so if the total number of readings
   * is small enough we need to compensate slightly when aggregating the spread
   * data from jobs with few reducers together with jobs with many reducers.
   */
  private static final long SMALL_SPREAD_COMPENSATION_THRESHOLD = 5L;

  /**
   * {@code MAXIMUM_CLOCK_SKEW} is the maximum plausible difference between the
   * clocks of machines in the same cluster. This is important because an event
   * that logically must follow a second event will be considered non-anomalous
   * if it precedes that second event, provided they happen on different
   * machines.
   */
  private static final long MAXIMUM_CLOCK_SKEW = 10000L;

  /**
   * The regular expression used to parse task attempt IDs in job tracker logs
   */
  private final static Pattern taskAttemptIDPattern = Pattern
      .compile(".*_([0-9]+)");

  private final static Pattern xmlFilePrefix = Pattern.compile("[ \t]*<");

  private final static Pattern confFileHeader = Pattern.compile("_conf.xml!!");

  private final Map<String, Pattern> counterPatterns = new HashMap<String, Pattern>();

  /**
   * The unpaired job config file. Currently only used to glean the {@code -Xmx}
   * field of the JRE options
   */
  private ParsedConfigFile jobconf = null;

  /**
   * Set by {@code -omit-task-details}. If true, we <i>only</i> emit the job
   * digest [statistical info], not the detailed job trace.
   */
  private boolean omitTaskDetails = false;

  private JsonGenerator jobTraceGen = null;

  private boolean prettyprintTrace = true;

  private LoggedJob jobBeingTraced = null;

  private Map<String, LoggedTask> tasksInCurrentJob;

  private Map<String, LoggedTaskAttempt> attemptsInCurrentJob;

  private Histogram[] successfulMapAttemptTimes;
  private Histogram successfulReduceAttemptTimes;
  private Histogram[] failedMapAttemptTimes;
  private Histogram failedReduceAttemptTimes;
  private Histogram successfulNthMapperAttempts;
  private Histogram successfulNthReducerAttempts;
  private Histogram mapperLocality;

  static final private Log LOG = LogFactory.getLog(HadoopLogsAnalyzer.class);

  private int[] attemptTimesPercentiles;

  private JsonGenerator topologyGen = null;

  private HashSet<ParsedHost> allHosts = new HashSet<ParsedHost>();

  // number of ticks per second
  private boolean collecting = false;

  private long lineNumber = 0;

  private String rereadableLine = null;

  private String inputFilename;

  private boolean inputIsDirectory = false;

  private Path inputDirectoryPath = null;
  private String[] inputDirectoryFiles = null;

  private int inputDirectoryCursor = -1;

  private LineReader input = null;
  private CompressionCodec inputCodec = null;
  private Decompressor inputDecompressor = null;
  private Text inputLineText = new Text();

  private boolean debug = false;

  private int version = 0;

  private int numberBuckets = 99;

  private int spreadMin;

  private int spreadMax;

  private boolean spreading = false;
  private boolean delays = false;
  private boolean runtimes = false;

  private boolean collectTaskTimes = false;

  private LogRecordType canonicalJob = LogRecordType.intern("Job");
  private LogRecordType canonicalMapAttempt = LogRecordType
      .intern("MapAttempt");
  private LogRecordType canonicalReduceAttempt = LogRecordType
      .intern("ReduceAttempt");
  private LogRecordType canonicalTask = LogRecordType.intern("Task");

  private static Pattern streamingJobnamePattern = Pattern
      .compile("streamjob\\d+.jar");

  private HashSet<String> hostNames = new HashSet<String>();

  private boolean fileFirstLine = true;
  private String currentFileName = null;

  // Here are the cumulative statistics.
  enum JobOutcome {
    SUCCESS, FAILURE, OVERALL
  };

  /**
   * These rectangular arrays of {@link Histogram}s are indexed by the job type
   * [java, streaming, pig or pipes] and then by the outcome [success or
   * failure]
   */
  private Histogram runTimeDists[][];
  private Histogram delayTimeDists[][];
  private Histogram mapTimeSpreadDists[][];
  private Histogram shuffleTimeSpreadDists[][];
  private Histogram sortTimeSpreadDists[][];
  private Histogram reduceTimeSpreadDists[][];

  private Histogram mapTimeDists[][];
  private Histogram shuffleTimeDists[][];
  private Histogram sortTimeDists[][];
  private Histogram reduceTimeDists[][];

  private Map<String, Long> taskAttemptStartTimes;
  private Map<String, Long> taskReduceAttemptShuffleEndTimes;
  private Map<String, Long> taskReduceAttemptSortEndTimes;
  private Map<String, Long> taskMapAttemptFinishTimes;
  private Map<String, Long> taskReduceAttemptFinishTimes;

  private long submitTimeCurrentJob;
  private long launchTimeCurrentJob;

  private String currentJobID;

  // TODO this is currently not being set correctly. We should fix it.
  // That only matters for statistics extraction.
  private LoggedJob.JobType thisJobType;

  private Histogram[][] newDistributionBlock() {
    return newDistributionBlock(null);
  }

  private Histogram[][] newDistributionBlock(String blockname) {
    Histogram[][] result = new Histogram[JobOutcome.values().length][];

    for (int i = 0; i < JobOutcome.values().length; ++i) {
      result[i] = new Histogram[LoggedJob.JobType.values().length];

      for (int j = 0; j < LoggedJob.JobType.values().length; ++j) {
        result[i][j] = blockname == null ? new Histogram() : new Histogram(
            blockname);
      }
    }

    return result;
  }

  private Histogram getDistribution(Histogram[][] block, JobOutcome outcome,
      LoggedJob.JobType type) {
    return block[outcome.ordinal()][type.ordinal()];
  }

  private void usage() {
    statusOutput
        .print("Usage: \n"
            + "administrative subcommands:\n"
            + "-v1                  specify version 1 of the jt logs\n"
            + "-h or -help          print this message\n"
            + "-d or -debug         print voluminous debug info during processing\n"
            + "-collect-prefixes    collect the prefixes of log lines\n\n"
            + "  job trace subcommands\n"
            + "-write-job-trace     takes a filename.\n"
            + "                     writes job trace in JSON to that filename\n"
            + "-single-line-job-traces  omit prettyprinting of job trace\n"
            + "-omit-task-details   leave out info about each task and attempt,\n"
            + "                     so only statistical info is added to each job\n"
            + "-write-topology      takes a filename.\n"
            + "                     writes JSON file giving network topology\n"
            + "-job-digest-spectra  takes a list of percentile points\n"
            + "                     writes CDFs with min, max, and those percentiles\n\n"
            + "subcommands for task statistical info\n"
            + "-spreads             we have a mode where, for each job, we can\n"
            + "                     develop the ratio of percentile B to percentile A\n"
            + "                     of task run times.  Having developed that ratio,\n"
            + "                     we can consider it to be a datum and we can\n"
            + "                     build a CDF of those ratios.  -spreads turns\n"
            + "                     this option on, and takes A and B\n"
            + "-delays              tells us to gather and print CDFs for delays\n"
            + "                     from job submit to job start\n"
            + "-runtimes            prints CDFs of job wallclock times [launch\n"
            + "                     to finish]\n"
            + "-tasktimes           prints CDFs of job wallclock times [launch\n"
            + "                     to finish]\n\n");
  }

  public HadoopLogsAnalyzer() {
    super();
  }

  private boolean pathIsDirectory(Path p) throws IOException {
    FileSystem fs = p.getFileSystem(getConf());
    return fs.getFileStatus(p).isDir();
  }

  /**
   * @param args
   *          string arguments. See {@code usage()}
   * @throws FileNotFoundException
   * @throws IOException
   */
  private int initializeHadoopLogsAnalyzer(String[] args)
      throws FileNotFoundException, IOException {
    Path jobTraceFilename = null;
    Path topologyFilename = null;
    if (args.length == 0 || args[args.length - 1].charAt(0) == '-') {
      throw new IllegalArgumentException("No input specified.");
    } else {
      inputFilename = args[args.length - 1];
    }

    for (int i = 0; i < args.length - (inputFilename == null ? 0 : 1); ++i) {
      if ("-h".equals(args[i].toLowerCase())
          || "-help".equals(args[i].toLowerCase())) {
        usage();
        return 0;
      }

      if ("-c".equals(args[i].toLowerCase())
          || "-collect-prefixes".equals(args[i].toLowerCase())) {
        collecting = true;
        continue;
      }

      // these control the job digest
      if ("-write-job-trace".equals(args[i].toLowerCase())) {
        ++i;
        jobTraceFilename = new Path(args[i]);
        continue;
      }

      if ("-single-line-job-traces".equals(args[i].toLowerCase())) {
        prettyprintTrace = false;
        continue;
      }

      if ("-omit-task-details".equals(args[i].toLowerCase())) {
        omitTaskDetails = true;
        continue;
      }

      if ("-write-topology".equals(args[i].toLowerCase())) {
        ++i;
        topologyFilename = new Path(args[i]);
        continue;
      }

      if ("-job-digest-spectra".equals(args[i].toLowerCase())) {
        ArrayList<Integer> values = new ArrayList<Integer>();

        ++i;

        while (i < args.length && Character.isDigit(args[i].charAt(0))) {
          values.add(Integer.parseInt(args[i]));
          ++i;
        }

        if (values.size() == 0) {
          throw new IllegalArgumentException("Empty -job-digest-spectra list");
        }

        attemptTimesPercentiles = new int[values.size()];

        int lastValue = 0;

        for (int j = 0; j < attemptTimesPercentiles.length; ++j) {
          if (values.get(j) <= lastValue || values.get(j) >= 100) {
            throw new IllegalArgumentException(
                "Bad -job-digest-spectra percentiles list");
          }
          attemptTimesPercentiles[j] = values.get(j);
        }

        --i;
        continue;
      }

      if ("-d".equals(args[i].toLowerCase())
          || "-debug".equals(args[i].toLowerCase())) {
        debug = true;
        continue;
      }

      if ("-spreads".equals(args[i].toLowerCase())) {
        int min = Integer.parseInt(args[i + 1]);
        int max = Integer.parseInt(args[i + 2]);

        if (min < max && min < 1000 && max < 1000) {
          spreadMin = min;
          spreadMax = max;
          spreading = true;
          i += 2;
        }
        continue;
      }

      // These control log-wide CDF outputs
      if ("-delays".equals(args[i].toLowerCase())) {
        delays = true;
        continue;
      }

      if ("-runtimes".equals(args[i].toLowerCase())) {
        runtimes = true;
        continue;
      }

      if ("-tasktimes".equals(args[i].toLowerCase())) {
        collectTaskTimes = true;
        continue;
      }

      if ("-v1".equals(args[i].toLowerCase())) {
        version = 1;
        continue;
      }

      throw new IllegalArgumentException("Unrecognized argument: " + args[i]);
    }

    runTimeDists = newDistributionBlock();
    delayTimeDists = newDistributionBlock();
    mapTimeSpreadDists = newDistributionBlock("map-time-spreads");
    shuffleTimeSpreadDists = newDistributionBlock();
    sortTimeSpreadDists = newDistributionBlock();
    reduceTimeSpreadDists = newDistributionBlock();

    mapTimeDists = newDistributionBlock();
    shuffleTimeDists = newDistributionBlock();
    sortTimeDists = newDistributionBlock();
    reduceTimeDists = newDistributionBlock();

    taskAttemptStartTimes = new HashMap<String, Long>();
    taskReduceAttemptShuffleEndTimes = new HashMap<String, Long>();
    taskReduceAttemptSortEndTimes = new HashMap<String, Long>();
    taskMapAttemptFinishTimes = new HashMap<String, Long>();
    taskReduceAttemptFinishTimes = new HashMap<String, Long>();

    final Path inputPath = new Path(inputFilename);

    inputIsDirectory = pathIsDirectory(inputPath);

    if (jobTraceFilename != null && attemptTimesPercentiles == null) {
      attemptTimesPercentiles = new int[19];

      for (int i = 0; i < 19; ++i) {
        attemptTimesPercentiles[i] = (i + 1) * 5;
      }
    }

    if (!inputIsDirectory) {
      input = maybeUncompressedPath(inputPath);
    } else {
      inputDirectoryPath = inputPath;
      FileSystem fs = inputPath.getFileSystem(getConf());
      FileStatus[] statuses = fs.listStatus(inputPath);
      inputDirectoryFiles = new String[statuses.length];

      for (int i = 0; i < statuses.length; ++i) {
        inputDirectoryFiles[i] = statuses[i].getPath().getName();
      }

      // filter out the .crc files, if any
      int dropPoint = 0;

      for (int i = 0; i < inputDirectoryFiles.length; ++i) {
        String name = inputDirectoryFiles[i];

        if (!(name.length() >= 4 && ".crc".equals(name
            .substring(name.length() - 4)))) {
          inputDirectoryFiles[dropPoint++] = name;
        }
      }

      LOG.info("We dropped " + (inputDirectoryFiles.length - dropPoint)
          + " crc files.");

      String[] new_inputDirectoryFiles = new String[dropPoint];
      System.arraycopy(inputDirectoryFiles, 0, new_inputDirectoryFiles, 0,
          dropPoint);
      inputDirectoryFiles = new_inputDirectoryFiles;

      Arrays.sort(inputDirectoryFiles);

      if (!setNextDirectoryInputStream()) {
        throw new FileNotFoundException("Empty directory specified.");
      }
    }

    if (jobTraceFilename != null) {
      ObjectMapper jmapper = new ObjectMapper();
      jmapper.configure(
          SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
      JsonFactory jfactory = jmapper.getJsonFactory();
      FileSystem jobFS = jobTraceFilename.getFileSystem(getConf());
      jobTraceGen = jfactory.createJsonGenerator(
          jobFS.create(jobTraceFilename), JsonEncoding.UTF8);
      if (prettyprintTrace) {
        jobTraceGen.useDefaultPrettyPrinter();
      }

      if (topologyFilename != null) {
        ObjectMapper tmapper = new ObjectMapper();
        tmapper.configure(
            SerializationConfig.Feature.CAN_OVERRIDE_ACCESS_MODIFIERS, true);
        JsonFactory tfactory = tmapper.getJsonFactory();
        FileSystem topoFS = topologyFilename.getFileSystem(getConf());
        topologyGen = tfactory.createJsonGenerator(
            topoFS.create(topologyFilename), JsonEncoding.UTF8);
        topologyGen.useDefaultPrettyPrinter();
      }
    }

    return 0;
  }

  private LineReader maybeUncompressedPath(Path p)
      throws FileNotFoundException, IOException {
    CompressionCodecFactory codecs = new CompressionCodecFactory(getConf());
    inputCodec = codecs.getCodec(p);
    FileSystem fs = p.getFileSystem(getConf());
    FSDataInputStream fileIn = fs.open(p);

    if (inputCodec == null) {
      return new LineReader(fileIn, getConf());
    } else {
      inputDecompressor = CodecPool.getDecompressor(inputCodec);
      return new LineReader(inputCodec.createInputStream(fileIn,
          inputDecompressor), getConf());
    }
  }

  private boolean setNextDirectoryInputStream() throws FileNotFoundException,
      IOException {
    if (input != null) {
      input.close();
      LOG.info("File closed: "+currentFileName);
      input = null;
    }

    if (inputCodec != null) {
      CodecPool.returnDecompressor(inputDecompressor);
      inputDecompressor = null;
      inputCodec = null;
    }

    ++inputDirectoryCursor;

    if (inputDirectoryCursor >= inputDirectoryFiles.length) {
      return false;
    }

    fileFirstLine = true;

    currentFileName = inputDirectoryFiles[inputDirectoryCursor];

    LOG.info("\nOpening file " + currentFileName
        + "  *************************** .");
    LOG
        .info("This file, " + (inputDirectoryCursor + 1) + "/"
            + inputDirectoryFiles.length + ", starts with line " + lineNumber
            + ".");

    input = maybeUncompressedPath(new Path(inputDirectoryPath, currentFileName));

    return input != null;
  }

  private String readInputLine() throws IOException {
    try {
      if (input == null) {
        return null;
      }
      inputLineText.clear();
      if (input.readLine(inputLineText) == 0) {
        return null;
      }

      return inputLineText.toString();
    } catch (EOFException e) {
      return null;
    }

  }

  private String readCountedLine() throws IOException {
    if (rereadableLine != null) {
      String result = rereadableLine;
      rereadableLine = null;
      return result;
    }

    String result = readInputLine();

    if (result != null) {
      if (fileFirstLine && (result.equals("") || result.charAt(0) != '\f')) {
        fileFirstLine = false;
        rereadableLine = result;
        return "\f!!FILE " + currentFileName + "!!\n";
      }
      fileFirstLine = false;
      ++lineNumber;
    } else if (inputIsDirectory && setNextDirectoryInputStream()) {
      result = readCountedLine();
    }

    return result;
  }

  private void unreadCountedLine(String unreadee) {
    if (rereadableLine == null) {
      rereadableLine = unreadee;
    }
  }

  private boolean apparentConfFileHeader(String header) {
    return confFileHeader.matcher(header).find();
  }

  private boolean apparentXMLFileStart(String line) {
    return xmlFilePrefix.matcher(line).lookingAt();
  }

  // This can return either the Pair of the !!file line and the XMLconf
  // file, or null and an ordinary line. Returns just null if there's
  // no more input.
  private Pair<String, String> readBalancedLine() throws IOException {
    String line = readCountedLine();

    if (line == null) {
      return null;
    }

    while (line.indexOf('\f') > 0) {
      line = line.substring(line.indexOf('\f'));
    }

    if (line.length() != 0 && line.charAt(0) == '\f') {
      String subjectLine = readCountedLine();

      if (subjectLine != null && subjectLine.length() != 0
          && apparentConfFileHeader(line) && apparentXMLFileStart(subjectLine)) {
        StringBuilder sb = new StringBuilder();

        while (subjectLine != null && subjectLine.indexOf('\f') > 0) {
          subjectLine = subjectLine.substring(subjectLine.indexOf('\f'));
        }

        while (subjectLine != null
            && (subjectLine.length() == 0 || subjectLine.charAt(0) != '\f')) {
          sb.append(subjectLine);
          subjectLine = readCountedLine();
        }

        if (subjectLine != null) {
          unreadCountedLine(subjectLine);
        }

        return new Pair<String, String>(line, sb.toString());
      }

      // here we had a file line, but it introduced a log segment, not
      // a conf file. We want to just ignore the file line.

      return readBalancedLine();
    }

    String endlineString = (version == 0 ? " " : " .");

    if (line.length() < endlineString.length()) {
      return new Pair<String, String>(null, line);
    }

    if (!endlineString.equals(line.substring(line.length()
        - endlineString.length()))) {
      StringBuilder sb = new StringBuilder(line);

      String addedLine;

      do {
        addedLine = readCountedLine();

        if (addedLine == null) {
          return new Pair<String, String>(null, sb.toString());
        }

        while (addedLine.indexOf('\f') > 0) {
          addedLine = addedLine.substring(addedLine.indexOf('\f'));
        }

        if (addedLine.length() > 0 && addedLine.charAt(0) == '\f') {
          unreadCountedLine(addedLine);
          return new Pair<String, String>(null, sb.toString());
        }

        sb.append("\n");
        sb.append(addedLine);
      } while (!endlineString.equals(addedLine.substring(addedLine.length()
          - endlineString.length())));

      line = sb.toString();
    }

    return new Pair<String, String>(null, line);
  }

  private void incorporateSpread(Histogram taskTimes, Histogram[][] spreadTo,
      JobOutcome outcome, LoggedJob.JobType jtype) {
    if (!spreading) {
      return;
    }

    if (taskTimes.getTotalCount() <= 1) {
      return;
    }

    // there are some literals here that probably should be options
    int[] endpoints = new int[2];

    endpoints[0] = spreadMin;
    endpoints[1] = spreadMax;

    long[] endpointKeys = taskTimes.getCDF(1000, endpoints);

    int smallResultOffset = (taskTimes.getTotalCount() < SMALL_SPREAD_COMPENSATION_THRESHOLD ? 1
        : 0);

    Histogram myTotal = spreadTo[outcome.ordinal()][jtype.ordinal()];

    long dividend = endpointKeys[2 + smallResultOffset];
    long divisor = endpointKeys[1 - smallResultOffset];

    if (divisor > 0) {
      long mytotalRatio = dividend * 1000000L / divisor;

      myTotal.enter(mytotalRatio);
    }
  }

  private void canonicalDistributionsEnter(Histogram[][] block,
      JobOutcome outcome, LoggedJob.JobType type, long value) {
    getDistribution(block, outcome, type).enter(value);
    getDistribution(block, JobOutcome.OVERALL, type).enter(value);
    getDistribution(block, outcome, LoggedJob.JobType.OVERALL).enter(value);
    getDistribution(block, JobOutcome.OVERALL, LoggedJob.JobType.OVERALL)
        .enter(value);
  }

  private void processJobLine(ParsedLine line) throws JsonProcessingException,
      IOException {
    try {
      if (version == 0 || version == 1) {
        // determine the job type if this is the declaration line
        String jobID = line.get("JOBID");

        String user = line.get("USER");

        String jobPriority = line.get("JOB_PRIORITY");

        String submitTime = line.get("SUBMIT_TIME");

        String jobName = line.get("JOBNAME");

        String launchTime = line.get("LAUNCH_TIME");

        String finishTime = line.get("FINISH_TIME");

        String status = line.get("JOB_STATUS");

        String totalMaps = line.get("TOTAL_MAPS");

        String totalReduces = line.get("TOTAL_REDUCES");

        /*
         * If the job appears new [the ID is different from the most recent one,
         * if any] we make a new LoggedJob.
         */
        if (jobID != null
            && jobTraceGen != null
            && (jobBeingTraced == null || !jobID.equals(jobBeingTraced
                .getJobID()))) {
          // push out the old job if there is one, even though it did't get
          // mated
          // with a conf.

          finalizeJob();

          jobBeingTraced = new LoggedJob(jobID);

          tasksInCurrentJob = new HashMap<String, LoggedTask>();
          attemptsInCurrentJob = new HashMap<String, LoggedTaskAttempt>();

          // initialize all the per-job statistics gathering places
          successfulMapAttemptTimes = new Histogram[ParsedHost
              .numberOfDistances() + 1];
          for (int i = 0; i < successfulMapAttemptTimes.length; ++i) {
            successfulMapAttemptTimes[i] = new Histogram();
          }

          successfulReduceAttemptTimes = new Histogram();
          failedMapAttemptTimes = new Histogram[ParsedHost.numberOfDistances() + 1];
          for (int i = 0; i < failedMapAttemptTimes.length; ++i) {
            failedMapAttemptTimes[i] = new Histogram();
          }

          failedReduceAttemptTimes = new Histogram();
          successfulNthMapperAttempts = new Histogram();
          successfulNthReducerAttempts = new Histogram();
          mapperLocality = new Histogram();
        }

        // here we fill in all the stuff the trace might need
        if (jobBeingTraced != null) {
          if (user != null) {
            jobBeingTraced.setUser(user);
          }

          if (jobPriority != null) {
            jobBeingTraced.setPriority(LoggedJob.JobPriority
                .valueOf(jobPriority));
          }

          if (totalMaps != null) {
            jobBeingTraced.setTotalMaps(Integer.parseInt(totalMaps));
          }

          if (totalReduces != null) {
            jobBeingTraced.setTotalReduces(Integer.parseInt(totalReduces));
          }

          if (submitTime != null) {
            jobBeingTraced.setSubmitTime(Long.parseLong(submitTime));
          }

          if (launchTime != null) {
            jobBeingTraced.setLaunchTime(Long.parseLong(launchTime));
          }

          if (finishTime != null) {
            jobBeingTraced.setFinishTime(Long.parseLong(finishTime));
            if (status != null) {
              jobBeingTraced.setOutcome(Pre21JobHistoryConstants.Values.valueOf(status));
            }

            maybeMateJobAndConf();
          }
        }

        if (jobName != null) {
          // we'll make it java unless the name parses out
          Matcher m = streamingJobnamePattern.matcher(jobName);

          thisJobType = LoggedJob.JobType.JAVA;

          if (m.matches()) {
            thisJobType = LoggedJob.JobType.STREAMING;
          }
        }
        if (submitTime != null) {
          submitTimeCurrentJob = Long.parseLong(submitTime);

          currentJobID = jobID;

          taskAttemptStartTimes = new HashMap<String, Long>();
          taskReduceAttemptShuffleEndTimes = new HashMap<String, Long>();
          taskReduceAttemptSortEndTimes = new HashMap<String, Long>();
          taskMapAttemptFinishTimes = new HashMap<String, Long>();
          taskReduceAttemptFinishTimes = new HashMap<String, Long>();

          launchTimeCurrentJob = 0L;
        } else if (launchTime != null && jobID != null
            && currentJobID.equals(jobID)) {
          launchTimeCurrentJob = Long.parseLong(launchTime);
        } else if (finishTime != null && jobID != null
            && currentJobID.equals(jobID)) {
          long endTime = Long.parseLong(finishTime);

          if (launchTimeCurrentJob != 0) {
            String jobResultText = line.get("JOB_STATUS");

            JobOutcome thisOutcome = ((jobResultText != null && "SUCCESS"
                .equals(jobResultText)) ? JobOutcome.SUCCESS
                : JobOutcome.FAILURE);

            if (submitTimeCurrentJob != 0L) {
              canonicalDistributionsEnter(delayTimeDists, thisOutcome,
                  thisJobType, launchTimeCurrentJob - submitTimeCurrentJob);
            }

            if (launchTimeCurrentJob != 0L) {
              canonicalDistributionsEnter(runTimeDists, thisOutcome,
                  thisJobType, endTime - launchTimeCurrentJob);
            }

            // Now we process the hash tables with successful task attempts

            Histogram currentJobMapTimes = new Histogram();
            Histogram currentJobShuffleTimes = new Histogram();
            Histogram currentJobSortTimes = new Histogram();
            Histogram currentJobReduceTimes = new Histogram();

            Iterator<Map.Entry<String, Long>> taskIter = taskAttemptStartTimes
                .entrySet().iterator();

            while (taskIter.hasNext()) {
              Map.Entry<String, Long> entry = taskIter.next();

              long startTime = entry.getValue();

              // Map processing
              Long mapEndTime = taskMapAttemptFinishTimes.get(entry.getKey());

              if (mapEndTime != null) {
                currentJobMapTimes.enter(mapEndTime - startTime);

                canonicalDistributionsEnter(mapTimeDists, thisOutcome,
                    thisJobType, mapEndTime - startTime);
              }

              // Reduce processing
              Long shuffleEnd = taskReduceAttemptShuffleEndTimes.get(entry
                  .getKey());
              Long sortEnd = taskReduceAttemptSortEndTimes.get(entry.getKey());
              Long reduceEnd = taskReduceAttemptFinishTimes.get(entry.getKey());

              if (shuffleEnd != null && sortEnd != null && reduceEnd != null) {
                currentJobShuffleTimes.enter(shuffleEnd - startTime);
                currentJobSortTimes.enter(sortEnd - shuffleEnd);
                currentJobReduceTimes.enter(reduceEnd - sortEnd);

                canonicalDistributionsEnter(shuffleTimeDists, thisOutcome,
                    thisJobType, shuffleEnd - startTime);
                canonicalDistributionsEnter(sortTimeDists, thisOutcome,
                    thisJobType, sortEnd - shuffleEnd);
                canonicalDistributionsEnter(reduceTimeDists, thisOutcome,
                    thisJobType, reduceEnd - sortEnd);
              }
            }

            // Here we save out the task information
            incorporateSpread(currentJobMapTimes, mapTimeSpreadDists,
                thisOutcome, thisJobType);
            incorporateSpread(currentJobShuffleTimes, shuffleTimeSpreadDists,
                thisOutcome, thisJobType);
            incorporateSpread(currentJobSortTimes, sortTimeSpreadDists,
                thisOutcome, thisJobType);
            incorporateSpread(currentJobReduceTimes, reduceTimeSpreadDists,
                thisOutcome, thisJobType);
          }
        }
      }
    } catch (NumberFormatException e) {
      LOG.warn(
          "HadoopLogsAnalyzer.processJobLine: bad numerical format, at line "
              + lineNumber + ".", e);
    }
  }

  private void processTaskLine(ParsedLine line) {
    if (jobBeingTraced != null) {
      // these fields are in both the start and finish record
      String taskID = line.get("TASKID");
      String taskType = line.get("TASK_TYPE");

      // this field is only in the start record
      String startTime = line.get("START_TIME");

      // these fields only exist or are only relevant in the finish record
      String status = line.get("TASK_STATUS");
      String finishTime = line.get("FINISH_TIME");

      String splits = line.get("SPLITS");

      LoggedTask task = tasksInCurrentJob.get(taskID);

      boolean taskAlreadyLogged = task != null;

      if (task == null) {
        task = new LoggedTask();
      }

      if (splits != null) {
        ArrayList<LoggedLocation> locations = null;

        StringTokenizer tok = new StringTokenizer(splits, ",", false);

        if (tok.countTokens() <= MAXIMUM_PREFERRED_LOCATIONS) {
          locations = new ArrayList<LoggedLocation>();
        }

        while (tok.hasMoreTokens()) {
          String nextSplit = tok.nextToken();

          ParsedHost node = getAndRecordParsedHost(nextSplit);

          if (locations != null && node != null) {
            locations.add(node.makeLoggedLocation());
          }
        }

        task.setPreferredLocations(locations);
      }

      task.setTaskID(taskID);

      if (startTime != null) {
        task.setStartTime(Long.parseLong(startTime));
      }

      if (finishTime != null) {
        task.setFinishTime(Long.parseLong(finishTime));
      }

      Pre21JobHistoryConstants.Values typ;
      Pre21JobHistoryConstants.Values stat;

      try {
        stat = status == null ? null : Pre21JobHistoryConstants.Values.valueOf(status);
      } catch (IllegalArgumentException e) {
        LOG.error("A task status you don't know about is \"" + status + "\".",
            e);
        stat = null;
      }

      task.setTaskStatus(stat);

      try {
        typ = taskType == null ? null : Pre21JobHistoryConstants.Values.valueOf(taskType);
      } catch (IllegalArgumentException e) {
        LOG.error("A task type you don't know about is \"" + taskType + "\".",
            e);
        typ = null;
      }
      
      if (typ == null) {
        return;
      }

      task.setTaskType(typ);

      List<LoggedTask> vec = typ == Pre21JobHistoryConstants.Values.MAP ? jobBeingTraced
          .getMapTasks() : typ == Pre21JobHistoryConstants.Values.REDUCE ? jobBeingTraced
          .getReduceTasks() : jobBeingTraced.getOtherTasks();

      if (!taskAlreadyLogged) {
        vec.add(task);

        tasksInCurrentJob.put(taskID, task);
      }
    }
  }

  private Pattern counterPattern(String counterName) {
    Pattern result = counterPatterns.get(counterName);

    if (result == null) {
      String namePatternRegex = "\\[\\(" + counterName
          + "\\)\\([^)]+\\)\\(([0-9]+)\\)\\]";
      result = Pattern.compile(namePatternRegex);
      counterPatterns.put(counterName, result);
    }

    return result;
  }

  private String parseCounter(String counterString, String counterName) {
    if (counterString == null) {
      return null;
    }

    Matcher mat = counterPattern(counterName).matcher(counterString);

    if (mat.find()) {
      return mat.group(1);
    }

    return null;
  }

  abstract class SetField {
    LoggedTaskAttempt attempt;

    SetField(LoggedTaskAttempt attempt) {
      this.attempt = attempt;
    }

    abstract void set(long value);
  }

  private void incorporateCounter(SetField thunk, String counterString,
      String counterName) {
    String valueString = parseCounter(counterString, counterName);

    if (valueString != null) {
      thunk.set(Long.parseLong(valueString));
    }
  }

  private void incorporateCounters(LoggedTaskAttempt attempt2,
      String counterString) {
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.hdfsBytesRead = val;
      }
    }, counterString, "HDFS_BYTES_READ");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.hdfsBytesWritten = val;
      }
    }, counterString, "HDFS_BYTES_WRITTEN");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.fileBytesRead = val;
      }
    }, counterString, "FILE_BYTES_READ");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.fileBytesWritten = val;
      }
    }, counterString, "FILE_BYTES_WRITTEN");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.mapInputBytes = val;
      }
    }, counterString, "MAP_INPUT_BYTES");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.mapInputRecords = val;
      }
    }, counterString, "MAP_INPUT_RECORDS");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.mapOutputBytes = val;
      }
    }, counterString, "MAP_OUTPUT_BYTES");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.mapOutputRecords = val;
      }
    }, counterString, "MAP_OUTPUT_RECORDS");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.combineInputRecords = val;
      }
    }, counterString, "COMBINE_INPUT_RECORDS");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.reduceInputGroups = val;
      }
    }, counterString, "REDUCE_INPUT_GROUPS");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.reduceInputRecords = val;
      }
    }, counterString, "REDUCE_INPUT_RECORDS");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.reduceShuffleBytes = val;
      }
    }, counterString, "REDUCE_SHUFFLE_BYTES");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.reduceOutputRecords = val;
      }
    }, counterString, "REDUCE_OUTPUT_RECORDS");
    incorporateCounter(new SetField(attempt2) {
      @Override
      void set(long val) {
        attempt.spilledRecords = val;
      }
    }, counterString, "SPILLED_RECORDS");
  }

  private ParsedHost getAndRecordParsedHost(String hostName) {
    ParsedHost result = ParsedHost.parse(hostName);

    if (result != null && !allHosts.contains(result)) {
      allHosts.add(result);
    }

    return result;
  }

  private void processMapAttemptLine(ParsedLine line) {
    String attemptID = line.get("TASK_ATTEMPT_ID");

    String taskID = line.get("TASKID");

    String status = line.get("TASK_STATUS");

    String attemptStartTime = line.get("START_TIME");
    String attemptFinishTime = line.get("FINISH_TIME");

    String hostName = line.get("HOSTNAME");

    String counters = line.get("COUNTERS");

    if (jobBeingTraced != null && taskID != null) {
      LoggedTask task = tasksInCurrentJob.get(taskID);

      if (task == null) {
        task = new LoggedTask();

        task.setTaskID(taskID);

        jobBeingTraced.getMapTasks().add(task);

        tasksInCurrentJob.put(taskID, task);
      }

      task.setTaskID(taskID);

      LoggedTaskAttempt attempt = attemptsInCurrentJob.get(attemptID);

      boolean attemptAlreadyExists = attempt != null;

      if (attempt == null) {
        attempt = new LoggedTaskAttempt();

        attempt.setAttemptID(attemptID);
      }

      if (!attemptAlreadyExists) {
        attemptsInCurrentJob.put(attemptID, attempt);
        task.getAttempts().add(attempt);
      }

      Pre21JobHistoryConstants.Values stat = null;

      try {
        stat = status == null ? null : Pre21JobHistoryConstants.Values.valueOf(status);
      } catch (IllegalArgumentException e) {
        LOG.error("A map attempt status you don't know about is \"" + status
            + "\".", e);
        stat = null;
      }

      incorporateCounters(attempt, counters);

      attempt.setResult(stat);

      if (attemptStartTime != null) {
        attempt.setStartTime(Long.parseLong(attemptStartTime));
      }

      if (attemptFinishTime != null) {
        attempt.setFinishTime(Long.parseLong(attemptFinishTime));
      }

      int distance = Integer.MAX_VALUE;

      if (hostName != null) {
        attempt.setHostName(hostName);

        ParsedHost host = null;

        host = getAndRecordParsedHost(hostName);

        if (host != null) {
          attempt.setLocation(host.makeLoggedLocation());
        }

        List<LoggedLocation> locs = task.getPreferredLocations();

        if (host != null && locs != null) {
          for (LoggedLocation loc : locs) {
            ParsedHost preferedLoc = new ParsedHost(loc);

            distance = Math.min(distance, preferedLoc.distance(host));
          }
        }

        mapperLocality.enter(distance);
      }

      distance = Math.min(distance, successfulMapAttemptTimes.length - 1);

      if (attempt.getStartTime() > 0 && attempt.getFinishTime() > 0) {
        long runtime = attempt.getFinishTime() - attempt.getStartTime();

        if (stat == Pre21JobHistoryConstants.Values.SUCCESS) {
          successfulMapAttemptTimes[distance].enter(runtime);
        }

        if (stat == Pre21JobHistoryConstants.Values.FAILED) {
          failedMapAttemptTimes[distance].enter(runtime);
        }
      }

      if (attemptID != null) {
        Matcher matcher = taskAttemptIDPattern.matcher(attemptID);

        if (matcher.matches()) {
          String attemptNumberString = matcher.group(1);

          if (attemptNumberString != null) {
            int attemptNumber = Integer.parseInt(attemptNumberString);

            successfulNthMapperAttempts.enter(attemptNumber);
          }
        }
      }
    }

    try {
      if (attemptStartTime != null) {
        long startTimeValue = Long.parseLong(attemptStartTime);

        if (startTimeValue != 0
            && startTimeValue + MAXIMUM_CLOCK_SKEW >= launchTimeCurrentJob) {
          taskAttemptStartTimes.put(attemptID, startTimeValue);
        } else {
          taskAttemptStartTimes.remove(attemptID);
        }
      } else if (status != null && attemptFinishTime != null) {
        long finishTime = Long.parseLong(attemptFinishTime);

        if (status.equals("SUCCESS")) {
          taskMapAttemptFinishTimes.put(attemptID, finishTime);
        }
      }
    } catch (NumberFormatException e) {
      LOG.warn(
          "HadoopLogsAnalyzer.processMapAttemptLine: bad numerical format, at line"
              + lineNumber + ".", e);
    }
  }

  private void processReduceAttemptLine(ParsedLine line) {
    String attemptID = line.get("TASK_ATTEMPT_ID");

    String taskID = line.get("TASKID");

    String status = line.get("TASK_STATUS");

    String attemptStartTime = line.get("START_TIME");
    String attemptFinishTime = line.get("FINISH_TIME");
    String attemptShuffleFinished = line.get("SHUFFLE_FINISHED");
    String attemptSortFinished = line.get("SORT_FINISHED");

    String counters = line.get("COUNTERS");

    String hostName = line.get("HOSTNAME");

    if (hostName != null && !hostNames.contains(hostName)) {
      hostNames.add(hostName);
    }

    if (jobBeingTraced != null && taskID != null) {
      LoggedTask task = tasksInCurrentJob.get(taskID);

      if (task == null) {
        task = new LoggedTask();

        task.setTaskID(taskID);

        jobBeingTraced.getReduceTasks().add(task);

        tasksInCurrentJob.put(taskID, task);
      }

      task.setTaskID(taskID);

      LoggedTaskAttempt attempt = attemptsInCurrentJob.get(attemptID);

      boolean attemptAlreadyExists = attempt != null;

      if (attempt == null) {
        attempt = new LoggedTaskAttempt();

        attempt.setAttemptID(attemptID);
      }

      if (!attemptAlreadyExists) {
        attemptsInCurrentJob.put(attemptID, attempt);
        task.getAttempts().add(attempt);
      }

      Pre21JobHistoryConstants.Values stat = null;

      try {
        stat = status == null ? null : Pre21JobHistoryConstants.Values.valueOf(status);
      } catch (IllegalArgumentException e) {
        LOG.warn("A map attempt status you don't know about is \"" + status
            + "\".", e);
        stat = null;
      }

      incorporateCounters(attempt, counters);

      attempt.setResult(stat);

      if (attemptStartTime != null) {
        attempt.setStartTime(Long.parseLong(attemptStartTime));
      }

      if (attemptFinishTime != null) {
        attempt.setFinishTime(Long.parseLong(attemptFinishTime));
      }

      if (attemptShuffleFinished != null) {
        attempt.setShuffleFinished(Long.parseLong(attemptShuffleFinished));
      }

      if (attemptSortFinished != null) {
        attempt.setSortFinished(Long.parseLong(attemptSortFinished));
      }

      if (attempt.getStartTime() > 0 && attempt.getFinishTime() > 0) {
        long runtime = attempt.getFinishTime() - attempt.getStartTime();

        if (stat == Pre21JobHistoryConstants.Values.SUCCESS) {
          successfulReduceAttemptTimes.enter(runtime);
        }

        if (stat == Pre21JobHistoryConstants.Values.FAILED) {
          failedReduceAttemptTimes.enter(runtime);
        }
      }
      if (hostName != null) {
        attempt.setHostName(hostName);
      }

      if (attemptID != null) {
        Matcher matcher = taskAttemptIDPattern.matcher(attemptID);

        if (matcher.matches()) {
          String attemptNumberString = matcher.group(1);

          if (attemptNumberString != null) {
            int attemptNumber = Integer.parseInt(attemptNumberString);

            successfulNthReducerAttempts.enter(attemptNumber);
          }
        }
      }
    }

    try {
      if (attemptStartTime != null) {
        long startTimeValue = Long.parseLong(attemptStartTime);

        if (startTimeValue != 0
            && startTimeValue + MAXIMUM_CLOCK_SKEW >= launchTimeCurrentJob) {
          taskAttemptStartTimes.put(attemptID, startTimeValue);
        }
      } else if (status != null && status.equals("SUCCESS")
          && attemptFinishTime != null) {
        long finishTime = Long.parseLong(attemptFinishTime);

        taskReduceAttemptFinishTimes.put(attemptID, finishTime);

        if (attemptShuffleFinished != null) {
          taskReduceAttemptShuffleEndTimes.put(attemptID, Long
              .parseLong(attemptShuffleFinished));
        }

        if (attemptSortFinished != null) {
          taskReduceAttemptSortEndTimes.put(attemptID, Long
              .parseLong(attemptSortFinished));
        }
      }
    } catch (NumberFormatException e) {
      LOG.error(
          "HadoopLogsAnalyzer.processReduceAttemptLine: bad numerical format, at line"
              + lineNumber + ".", e);
    }
  }

  private void processParsedLine(ParsedLine line)
      throws JsonProcessingException, IOException {
    if (!collecting) {
      // "Job", "MapAttempt", "ReduceAttempt", "Task"
      LogRecordType myType = line.getType();

      if (myType == canonicalJob) {
        processJobLine(line);
      } else if (myType == canonicalTask) {
        processTaskLine(line);
      } else if (myType == canonicalMapAttempt) {
        processMapAttemptLine(line);
      } else if (myType == canonicalReduceAttempt) {
        processReduceAttemptLine(line);
      } else {
      }
    }
  }

  private void printDistributionSet(String title, Histogram[][] distSet) {
    statisticalOutput.print(title + "\n\n");

    // print out buckets

    for (int i = 0; i < JobOutcome.values().length; ++i) {
      for (int j = 0; j < LoggedJob.JobType.values().length; ++j) {
        JobOutcome thisOutcome = JobOutcome.values()[i];
        LoggedJob.JobType thisType = LoggedJob.JobType.values()[j];

        statisticalOutput.print("outcome = ");
        statisticalOutput.print(thisOutcome.toString());
        statisticalOutput.print(", and type = ");
        statisticalOutput.print(thisType.toString());
        statisticalOutput.print(".\n\n");

        Histogram dist = distSet[i][j];

        printSingleDistributionData(dist);
      }
    }
  }

  private void printSingleDistributionData(Histogram dist) {
    int[] percentiles = new int[numberBuckets];

    for (int k = 0; k < numberBuckets; ++k) {
      percentiles[k] = k + 1;
    }

    long[] cdf = dist.getCDF(numberBuckets + 1, percentiles);

    if (cdf == null) {
      statisticalOutput.print("(No data)\n");
    } else {
      statisticalOutput.print("min:  ");
      statisticalOutput.print(cdf[0]);
      statisticalOutput.print("\n");

      for (int k = 0; k < numberBuckets; ++k) {
        statisticalOutput.print(percentiles[k]);
        statisticalOutput.print("%   ");
        statisticalOutput.print(cdf[k + 1]);
        statisticalOutput.print("\n");
      }

      statisticalOutput.print("max:  ");
      statisticalOutput.print(cdf[numberBuckets + 1]);
      statisticalOutput.print("\n");
    }
  }

  private void maybeMateJobAndConf() throws IOException {
    if (jobBeingTraced != null && jobconf != null
        && jobBeingTraced.getJobID().equals(jobconf.jobID)) {
      jobBeingTraced.setHeapMegabytes(jobconf.heapMegabytes);

      jobBeingTraced.setQueue(jobconf.queue);
      jobBeingTraced.setJobName(jobconf.jobName);

      jobBeingTraced.setClusterMapMB(jobconf.clusterMapMB);
      jobBeingTraced.setClusterReduceMB(jobconf.clusterReduceMB);
      jobBeingTraced.setJobMapMB(jobconf.jobMapMB);
      jobBeingTraced.setJobReduceMB(jobconf.jobReduceMB);

      jobconf = null;

      finalizeJob();
    }
  }

  private ArrayList<LoggedDiscreteCDF> mapCDFArrayList(Histogram[] data) {
    ArrayList<LoggedDiscreteCDF> result = new ArrayList<LoggedDiscreteCDF>();

    for (Histogram hist : data) {
      LoggedDiscreteCDF discCDF = new LoggedDiscreteCDF();
      discCDF.setCDF(hist, attemptTimesPercentiles, 100);
      result.add(discCDF);
    }

    return result;
  }

  private void finalizeJob() throws IOException {
    if (jobBeingTraced != null) {
      if (omitTaskDetails) {
        jobBeingTraced.setMapTasks(null);
        jobBeingTraced.setReduceTasks(null);
        jobBeingTraced.setOtherTasks(null);
      }

      // add digest info to the job
      jobBeingTraced
          .setSuccessfulMapAttemptCDFs(mapCDFArrayList(successfulMapAttemptTimes));
      jobBeingTraced
          .setFailedMapAttemptCDFs(mapCDFArrayList(failedMapAttemptTimes));

      LoggedDiscreteCDF discCDF = new LoggedDiscreteCDF();
      discCDF
          .setCDF(successfulReduceAttemptTimes, attemptTimesPercentiles, 100);
      jobBeingTraced.setSuccessfulReduceAttemptCDF(discCDF);

      discCDF = new LoggedDiscreteCDF();
      discCDF.setCDF(failedReduceAttemptTimes, attemptTimesPercentiles, 100);
      jobBeingTraced.setFailedReduceAttemptCDF(discCDF);

      long totalSuccessfulAttempts = 0L;
      long maxTriesToSucceed = 0L;

      for (Map.Entry<Long, Long> ent : successfulNthMapperAttempts) {
        totalSuccessfulAttempts += ent.getValue();
        maxTriesToSucceed = Math.max(maxTriesToSucceed, ent.getKey());
      }

      if (totalSuccessfulAttempts > 0L) {
        double[] successAfterI = new double[(int) maxTriesToSucceed + 1];
        for (int i = 0; i < successAfterI.length; ++i) {
          successAfterI[i] = 0.0D;
        }

        for (Map.Entry<Long, Long> ent : successfulNthMapperAttempts) {
          successAfterI[ent.getKey().intValue()] = ((double) ent.getValue())
              / totalSuccessfulAttempts;
        }
        jobBeingTraced.setMapperTriesToSucceed(successAfterI);
      } else {
        jobBeingTraced.setMapperTriesToSucceed(null);
      }

      jobTraceGen.writeObject(jobBeingTraced);

      jobTraceGen.writeRaw("\n");

      jobBeingTraced = null;
    }
  }

  public int run(String[] args) throws IOException {

    int result = initializeHadoopLogsAnalyzer(args);

    if (result != 0) {
      return result;
    }

    return run();
  }

  int run() throws IOException {
    Pair<String, String> line = readBalancedLine();

    while (line != null) {
      if (debug
          && (lineNumber < 1000000L && lineNumber % 1000L == 0 || lineNumber % 1000000L == 0)) {
        LOG.debug("" + lineNumber + " " + line.second());
      }

      if (line.first() == null) {
        try {
          // HACK ALERT!! It's possible for a Job end line to end a
          // job for which we have a config file
          // image [ a ParsedConfigFile ] in jobconf.
          //
          // processParsedLine handles this.

          processParsedLine(new ParsedLine(line.second(), version));
        } catch (StringIndexOutOfBoundsException e) {
          LOG.warn("anomalous line #" + lineNumber + ":" + line, e);
        }
      } else {
        jobconf = new ParsedConfigFile(line.first(), line.second());

        if (jobconf.valid == false) {
          jobconf = null;
        }

        maybeMateJobAndConf();
      }

      line = readBalancedLine();
    }

    finalizeJob();

    if (collecting) {
      String[] typeNames = LogRecordType.lineTypes();

      for (int i = 0; i < typeNames.length; ++i) {
        statisticalOutput.print(typeNames[i]);
        statisticalOutput.print('\n');
      }
    } else {
      if (delays) {
        printDistributionSet("Job start delay spectrum:", delayTimeDists);
      }

      if (runtimes) {
        printDistributionSet("Job run time spectrum:", runTimeDists);
      }

      if (spreading) {
        String ratioDescription = "(" + spreadMax + "/1000 %ile) to ("
            + spreadMin + "/1000 %ile) scaled by 1000000";

        printDistributionSet(
            "Map task success times " + ratioDescription + ":",
            mapTimeSpreadDists);
        printDistributionSet("Shuffle success times " + ratioDescription + ":",
            shuffleTimeSpreadDists);
        printDistributionSet("Sort success times " + ratioDescription + ":",
            sortTimeSpreadDists);
        printDistributionSet("Reduce success times " + ratioDescription + ":",
            reduceTimeSpreadDists);
      }

      if (collectTaskTimes) {
        printDistributionSet("Global map task success times:", mapTimeDists);
        printDistributionSet("Global shuffle task success times:",
            shuffleTimeDists);
        printDistributionSet("Global sort task success times:", sortTimeDists);
        printDistributionSet("Global reduce task success times:",
            reduceTimeDists);
      }
    }

    if (topologyGen != null) {
      LoggedNetworkTopology topo = new LoggedNetworkTopology(allHosts,
          "<root>", 0);
      topologyGen.writeObject(topo);
      topologyGen.close();
    }

    if (jobTraceGen != null) {
      jobTraceGen.close();
    }

    if (input != null) {
      input.close();
      input = null;
    }

    if (inputCodec != null) {
      CodecPool.returnDecompressor(inputDecompressor);
      inputDecompressor = null;
      inputCodec = null;
    }

    return 0;
  }

  /**
   * @param args
   * 
   *          Last arg is the input file. That file can be a directory, in which
   *          case you get all the files in sorted order. We will decompress
   *          files whose nmes end in .gz .
   * 
   *          switches: -c collect line types.
   * 
   *          -d debug mode
   * 
   *          -delays print out the delays [interval between job submit time and
   *          launch time]
   * 
   *          -runtimes print out the job runtimes
   * 
   *          -spreads print out the ratio of 10%ile and 90%ile, of both the
   *          successful map task attempt run times and the the successful
   *          reduce task attempt run times
   * 
   *          -tasktimes prints out individual task time distributions
   * 
   *          collects all the line types and prints the first example of each
   *          one
   */
  public static void main(String[] args) {
    try {
      HadoopLogsAnalyzer analyzer = new HadoopLogsAnalyzer();

      int result = ToolRunner.run(analyzer, args);

      if (result == 0) {
        return;
      }

      System.exit(result);
    } catch (FileNotFoundException e) {
      LOG.error("", e);
      e.printStackTrace(staticDebugOutput);
      System.exit(1);
    } catch (IOException e) {
      LOG.error("", e);
      e.printStackTrace(staticDebugOutput);
      System.exit(2);
    } catch (Exception e) {
      LOG.error("", e);
      e.printStackTrace(staticDebugOutput);
      System.exit(3);
    }
  }
}
