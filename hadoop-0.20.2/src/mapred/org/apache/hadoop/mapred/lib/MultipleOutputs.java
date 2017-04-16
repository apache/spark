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
package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.*;

/**
 * The MultipleOutputs class simplifies writting to additional outputs other
 * than the job default output via the <code>OutputCollector</code> passed to
 * the <code>map()</code> and <code>reduce()</code> methods of the
 * <code>Mapper</code> and <code>Reducer</code> implementations.
 * <p/>
 * Each additional output, or named output, may be configured with its own
 * <code>OutputFormat</code>, with its own key class and with its own value
 * class.
 * <p/>
 * A named output can be a single file or a multi file. The later is refered as
 * a multi named output.
 * <p/>
 * A multi named output is an unbound set of files all sharing the same
 * <code>OutputFormat</code>, key class and value class configuration.
 * <p/>
 * When named outputs are used within a <code>Mapper</code> implementation,
 * key/values written to a name output are not part of the reduce phase, only
 * key/values written to the job <code>OutputCollector</code> are part of the
 * reduce phase.
 * <p/>
 * MultipleOutputs supports counters, by default the are disabled. The counters
 * group is the {@link MultipleOutputs} class name.
 * </p>
 * The names of the counters are the same as the named outputs. For multi
 * named outputs the name of the counter is the concatenation of the named
 * output, and underscore '_' and the multiname.
 * <p/>
 * Job configuration usage pattern is:
 * <pre>
 *
 * JobConf conf = new JobConf();
 *
 * conf.setInputPath(inDir);
 * FileOutputFormat.setOutputPath(conf, outDir);
 *
 * conf.setMapperClass(MOMap.class);
 * conf.setReducerClass(MOReduce.class);
 * ...
 *
 * // Defines additional single text based output 'text' for the job
 * MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,
 * LongWritable.class, Text.class);
 *
 * // Defines additional multi sequencefile based output 'sequence' for the
 * // job
 * MultipleOutputs.addMultiNamedOutput(conf, "seq",
 *   SequenceFileOutputFormat.class,
 *   LongWritable.class, Text.class);
 * ...
 *
 * JobClient jc = new JobClient();
 * RunningJob job = jc.submitJob(conf);
 *
 * ...
 * </pre>
 * <p/>
 * Job configuration usage pattern is:
 * <pre>
 *
 * public class MOReduce implements
 *   Reducer&lt;WritableComparable, Writable&gt; {
 * private MultipleOutputs mos;
 *
 * public void configure(JobConf conf) {
 * ...
 * mos = new MultipleOutputs(conf);
 * }
 *
 * public void reduce(WritableComparable key, Iterator&lt;Writable&gt; values,
 * OutputCollector output, Reporter reporter)
 * throws IOException {
 * ...
 * mos.getCollector("text", reporter).collect(key, new Text("Hello"));
 * mos.getCollector("seq", "A", reporter).collect(key, new Text("Bye"));
 * mos.getCollector("seq", "B", reporter).collect(key, new Text("Chau"));
 * ...
 * }
 *
 * public void close() throws IOException {
 * mos.close();
 * ...
 * }
 *
 * }
 * </pre>
 */
public class MultipleOutputs {

  private static final String NAMED_OUTPUTS = "mo.namedOutputs";

  private static final String MO_PREFIX = "mo.namedOutput.";

  private static final String FORMAT = ".format";
  private static final String KEY = ".key";
  private static final String VALUE = ".value";
  private static final String MULTI = ".multi";

  private static final String COUNTERS_ENABLED = "mo.counters";

  /**
   * Counters group used by the counters of MultipleOutputs.
   */
  private static final String COUNTERS_GROUP = MultipleOutputs.class.getName();

  /**
   * Checks if a named output is alreadyDefined or not.
   *
   * @param conf           job conf
   * @param namedOutput    named output names
   * @param alreadyDefined whether the existence/non-existence of
   *                       the named output is to be checked
   * @throws IllegalArgumentException if the output name is alreadyDefined or
   *                                  not depending on the value of the
   *                                  'alreadyDefined' parameter
   */
  private static void checkNamedOutput(JobConf conf, String namedOutput,
                                       boolean alreadyDefined) {
    List<String> definedChannels = getNamedOutputsList(conf);
    if (alreadyDefined && definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' already alreadyDefined");
    } else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
      throw new IllegalArgumentException("Named output '" + namedOutput +
        "' not defined");
    }
  }

  /**
   * Checks if a named output name is valid token.
   *
   * @param namedOutput named output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkTokenName(String namedOutput) {
    if (namedOutput == null || namedOutput.length() == 0) {
      throw new IllegalArgumentException(
        "Name cannot be NULL or emtpy");
    }
    for (char ch : namedOutput.toCharArray()) {
      if ((ch >= 'A') && (ch <= 'Z')) {
        continue;
      }
      if ((ch >= 'a') && (ch <= 'z')) {
        continue;
      }
      if ((ch >= '0') && (ch <= '9')) {
        continue;
      }
      throw new IllegalArgumentException(
        "Name cannot be have a '" + ch + "' char");
    }
  }

  /**
   * Checks if a named output name is valid.
   *
   * @param namedOutput named output Name
   * @throws IllegalArgumentException if the output name is not valid.
   */
  private static void checkNamedOutputName(String namedOutput) {
    checkTokenName(namedOutput);
    // name cannot be the name used for the default output
    if (namedOutput.equals("part")) {
      throw new IllegalArgumentException(
        "Named output name cannot be 'part'");
    }
  }

  /**
   * Returns list of channel names.
   *
   * @param conf job conf
   * @return List of channel Names
   */
  public static List<String> getNamedOutputsList(JobConf conf) {
    List<String> names = new ArrayList<String>();
    StringTokenizer st = new StringTokenizer(conf.get(NAMED_OUTPUTS, ""), " ");
    while (st.hasMoreTokens()) {
      names.add(st.nextToken());
    }
    return names;
  }


  /**
   * Returns if a named output is multiple.
   *
   * @param conf        job conf
   * @param namedOutput named output
   * @return <code>true</code> if the name output is multi, <code>false</code>
   *         if it is single. If the name output is not defined it returns
   *         <code>false</code>
   */
  public static boolean isMultiNamedOutput(JobConf conf, String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getBoolean(MO_PREFIX + namedOutput + MULTI, false);
  }

  /**
   * Returns the named output OutputFormat.
   *
   * @param conf        job conf
   * @param namedOutput named output
   * @return namedOutput OutputFormat
   */
  public static Class<? extends OutputFormat> getNamedOutputFormatClass(
    JobConf conf, String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getClass(MO_PREFIX + namedOutput + FORMAT, null,
      OutputFormat.class);
  }

  /**
   * Returns the key class for a named output.
   *
   * @param conf        job conf
   * @param namedOutput named output
   * @return class for the named output key
   */
  public static Class<? extends WritableComparable> getNamedOutputKeyClass(JobConf conf,
                                                String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getClass(MO_PREFIX + namedOutput + KEY, null,
	WritableComparable.class);
  }

  /**
   * Returns the value class for a named output.
   *
   * @param conf        job conf
   * @param namedOutput named output
   * @return class of named output value
   */
  public static Class<? extends Writable> getNamedOutputValueClass(JobConf conf,
                                                  String namedOutput) {
    checkNamedOutput(conf, namedOutput, false);
    return conf.getClass(MO_PREFIX + namedOutput + VALUE, null,
      Writable.class);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   *
   * @param conf              job conf to add the named output
   * @param namedOutput       named output name, it has to be a word, letters
   *                          and numbers only, cannot be the word 'part' as
   *                          that is reserved for the
   *                          default output.
   * @param outputFormatClass OutputFormat class.
   * @param keyClass          key class
   * @param valueClass        value class
   */
  public static void addNamedOutput(JobConf conf, String namedOutput,
                                Class<? extends OutputFormat> outputFormatClass,
                                Class<?> keyClass, Class<?> valueClass) {
    addNamedOutput(conf, namedOutput, false, outputFormatClass, keyClass,
      valueClass);
  }

  /**
   * Adds a multi named output for the job.
   * <p/>
   *
   * @param conf              job conf to add the named output
   * @param namedOutput       named output name, it has to be a word, letters
   *                          and numbers only, cannot be the word 'part' as
   *                          that is reserved for the
   *                          default output.
   * @param outputFormatClass OutputFormat class.
   * @param keyClass          key class
   * @param valueClass        value class
   */
  public static void addMultiNamedOutput(JobConf conf, String namedOutput,
                               Class<? extends OutputFormat> outputFormatClass,
                               Class<?> keyClass, Class<?> valueClass) {
    addNamedOutput(conf, namedOutput, true, outputFormatClass, keyClass,
      valueClass);
  }

  /**
   * Adds a named output for the job.
   * <p/>
   *
   * @param conf              job conf to add the named output
   * @param namedOutput       named output name, it has to be a word, letters
   *                          and numbers only, cannot be the word 'part' as
   *                          that is reserved for the
   *                          default output.
   * @param multi             indicates if the named output is multi
   * @param outputFormatClass OutputFormat class.
   * @param keyClass          key class
   * @param valueClass        value class
   */
  private static void addNamedOutput(JobConf conf, String namedOutput,
                               boolean multi,
                               Class<? extends OutputFormat> outputFormatClass,
                               Class<?> keyClass, Class<?> valueClass) {
    checkNamedOutputName(namedOutput);
    checkNamedOutput(conf, namedOutput, true);
    conf.set(NAMED_OUTPUTS, conf.get(NAMED_OUTPUTS, "") + " " + namedOutput);
    conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass,
      OutputFormat.class);
    conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
    conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
    conf.setBoolean(MO_PREFIX + namedOutput + MULTI, multi);
  }

  /**
   * Enables or disables counters for the named outputs.
   * <p/>
   * By default these counters are disabled.
   * <p/>
   * MultipleOutputs supports counters, by default the are disabled.
   * The counters group is the {@link MultipleOutputs} class name.
   * </p>
   * The names of the counters are the same as the named outputs. For multi
   * named outputs the name of the counter is the concatenation of the named
   * output, and underscore '_' and the multiname.
   *
   * @param conf    job conf to enableadd the named output.
   * @param enabled indicates if the counters will be enabled or not.
   */
  public static void setCountersEnabled(JobConf conf, boolean enabled) {
    conf.setBoolean(COUNTERS_ENABLED, enabled);
  }

  /**
   * Returns if the counters for the named outputs are enabled or not.
   * <p/>
   * By default these counters are disabled.
   * <p/>
   * MultipleOutputs supports counters, by default the are disabled.
   * The counters group is the {@link MultipleOutputs} class name.
   * </p>
   * The names of the counters are the same as the named outputs. For multi
   * named outputs the name of the counter is the concatenation of the named
   * output, and underscore '_' and the multiname.
   *
   *
   * @param conf    job conf to enableadd the named output.
   * @return TRUE if the counters are enabled, FALSE if they are disabled.
   */
  public static boolean getCountersEnabled(JobConf conf) {
    return conf.getBoolean(COUNTERS_ENABLED, false);
  }

  // instance code, to be used from Mapper/Reducer code

  private JobConf conf;
  private OutputFormat outputFormat;
  private Set<String> namedOutputs;
  private Map<String, RecordWriter> recordWriters;
  private boolean countersEnabled;

  /**
   * Creates and initializes multiple named outputs support, it should be
   * instantiated in the Mapper/Reducer configure method.
   *
   * @param job the job configuration object
   */
  public MultipleOutputs(JobConf job) {
    this.conf = job;
    outputFormat = new InternalFileOutputFormat();
    namedOutputs = Collections.unmodifiableSet(
      new HashSet<String>(MultipleOutputs.getNamedOutputsList(job)));
    recordWriters = new HashMap<String, RecordWriter>();
    countersEnabled = getCountersEnabled(job);
  }

  /**
   * Returns iterator with the defined name outputs.
   *
   * @return iterator with the defined named outputs
   */
  public Iterator<String> getNamedOutputs() {
    return namedOutputs.iterator();
  }


  // by being synchronized MultipleOutputTask can be use with a
  // MultithreaderMapRunner.
  private synchronized RecordWriter getRecordWriter(String namedOutput,
                                                    String baseFileName,
                                                    final Reporter reporter)
    throws IOException {
    RecordWriter writer = recordWriters.get(baseFileName);
    if (writer == null) {
      if (countersEnabled && reporter == null) {
        throw new IllegalArgumentException(
          "Counters are enabled, Reporter cannot be NULL");
      }
      JobConf jobConf = new JobConf(conf);
      jobConf.set(InternalFileOutputFormat.CONFIG_NAMED_OUTPUT, namedOutput);
      FileSystem fs = FileSystem.get(conf);
      writer =
        outputFormat.getRecordWriter(fs, jobConf, baseFileName, reporter);

      if (countersEnabled) {
        if (reporter == null) {
          throw new IllegalArgumentException(
            "Counters are enabled, Reporter cannot be NULL");
        }
        writer = new RecordWriterWithCounter(writer, baseFileName, reporter);
      }

      recordWriters.put(baseFileName, writer);
    }
    return writer;
  }

  private static class RecordWriterWithCounter implements RecordWriter {
    private RecordWriter writer;
    private String counterName;
    private Reporter reporter;

    public RecordWriterWithCounter(RecordWriter writer, String counterName,
                                   Reporter reporter) {
      this.writer = writer;
      this.counterName = counterName;
      this.reporter = reporter;
    }

    @SuppressWarnings({"unchecked"})
    public void write(Object key, Object value) throws IOException {
      reporter.incrCounter(COUNTERS_GROUP, counterName, 1);
      writer.write(key, value);
    }

    public void close(Reporter reporter) throws IOException {
      writer.close(reporter);
    }
  }

  /**
   * Gets the output collector for a named output.
   * <p/>
   *
   * @param namedOutput the named output name
   * @param reporter    the reporter
   * @return the output collector for the given named output
   * @throws IOException thrown if output collector could not be created
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getCollector(String namedOutput, Reporter reporter)
    throws IOException {
    return getCollector(namedOutput, null, reporter);
  }

  /**
   * Gets the output collector for a multi named output.
   * <p/>
   *
   * @param namedOutput the named output name
   * @param multiName   the multi name part
   * @param reporter    the reporter
   * @return the output collector for the given named output
   * @throws IOException thrown if output collector could not be created
   */
  @SuppressWarnings({"unchecked"})
  public OutputCollector getCollector(String namedOutput, String multiName,
                                      Reporter reporter)
    throws IOException {

    checkNamedOutputName(namedOutput);
    if (!namedOutputs.contains(namedOutput)) {
      throw new IllegalArgumentException("Undefined named output '" +
        namedOutput + "'");
    }
    boolean multi = isMultiNamedOutput(conf, namedOutput);

    if (!multi && multiName != null) {
      throw new IllegalArgumentException("Name output '" + namedOutput +
        "' has not been defined as multi");
    }
    if (multi) {
      checkTokenName(multiName);
    }

    String baseFileName = (multi) ? namedOutput + "_" + multiName : namedOutput;

    final RecordWriter writer =
      getRecordWriter(namedOutput, baseFileName, reporter);

    return new OutputCollector() {

      @SuppressWarnings({"unchecked"})
      public void collect(Object key, Object value) throws IOException {
        writer.write(key, value);
      }

    };
  }

  /**
   * Closes all the opened named outputs.
   * <p/>
   * If overriden subclasses must invoke <code>super.close()</code> at the
   * end of their <code>close()</code>
   *
   * @throws java.io.IOException thrown if any of the MultipleOutput files
   *                             could not be closed properly.
   */
  public void close() throws IOException {
    for (RecordWriter writer : recordWriters.values()) {
      writer.close(null);
    }
  }

  private static class InternalFileOutputFormat extends
    FileOutputFormat<Object, Object> {

    public static final String CONFIG_NAMED_OUTPUT = "mo.config.namedOutput";

    @SuppressWarnings({"unchecked"})
    public RecordWriter<Object, Object> getRecordWriter(
      FileSystem fs, JobConf job, String baseFileName, Progressable progress)
      throws IOException {

      String nameOutput = job.get(CONFIG_NAMED_OUTPUT, null);
      String fileName = getUniqueName(job, baseFileName);

      // The following trick leverages the instantiation of a record writer via
      // the job conf thus supporting arbitrary output formats.
      JobConf outputConf = new JobConf(job);
      outputConf.setOutputFormat(getNamedOutputFormatClass(job, nameOutput));
      outputConf.setOutputKeyClass(getNamedOutputKeyClass(job, nameOutput));
      outputConf.setOutputValueClass(getNamedOutputValueClass(job, nameOutput));
      OutputFormat outputFormat = outputConf.getOutputFormat();
      return outputFormat.getRecordWriter(fs, outputConf, fileName, progress);
    }
  }

}
