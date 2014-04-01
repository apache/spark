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

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GenericMRLoadGenerator extends Configured implements Tool {

  protected static int printUsage() {
    System.err.println(
    "Usage: [-m <maps>] [-r <reduces>]\n" +
    "       [-keepmap <percent>] [-keepred <percent>]\n" +
    "       [-indir <path>] [-outdir <path]\n" +
    "       [-inFormat[Indirect] <InputFormat>] [-outFormat <OutputFormat>]\n" +
    "       [-outKey <WritableComparable>] [-outValue <Writable>]\n");
    GenericOptionsParser.printGenericCommandUsage(System.err);
    return -1;
  }


  /**
   * Configure a job given argv.
   */
  public static boolean parseArgs(String[] argv, JobConf job) throws IOException {
    if (argv.length < 1) {
      return 0 == printUsage();
    }
    for(int i=0; i < argv.length; ++i) {
      if (argv.length == i + 1) {
        System.out.println("ERROR: Required parameter missing from " +
            argv[i]);
        return 0 == printUsage();
      }
      try {
        if ("-m".equals(argv[i])) {
          job.setNumMapTasks(Integer.parseInt(argv[++i]));
        } else if ("-r".equals(argv[i])) {
          job.setNumReduceTasks(Integer.parseInt(argv[++i]));
        } else if ("-inFormat".equals(argv[i])) {
          job.setInputFormat(
              Class.forName(argv[++i]).asSubclass(InputFormat.class));
        } else if ("-outFormat".equals(argv[i])) {
          job.setOutputFormat(
              Class.forName(argv[++i]).asSubclass(OutputFormat.class));
        } else if ("-outKey".equals(argv[i])) {
          job.setOutputKeyClass(
            Class.forName(argv[++i]).asSubclass(WritableComparable.class));
        } else if ("-outValue".equals(argv[i])) {
          job.setOutputValueClass(
            Class.forName(argv[++i]).asSubclass(Writable.class));
        } else if ("-keepmap".equals(argv[i])) {
          job.set("hadoop.sort.map.keep.percent", argv[++i]);
        } else if ("-keepred".equals(argv[i])) {
          job.set("hadoop.sort.reduce.keep.percent", argv[++i]);
        } else if ("-outdir".equals(argv[i])) {
          FileOutputFormat.setOutputPath(job, new Path(argv[++i]));
        } else if ("-indir".equals(argv[i])) {
          FileInputFormat.addInputPaths(job, argv[++i]);
        } else if ("-inFormatIndirect".equals(argv[i])) {
          job.setClass("mapred.indirect.input.format",
              Class.forName(argv[++i]).asSubclass(InputFormat.class),
              InputFormat.class);
          job.setInputFormat(IndirectInputFormat.class);
        } else {
          System.out.println("Unexpected argument: " + argv[i]);
          return 0 == printUsage();
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + argv[i]);
        return 0 == printUsage();
      } catch (Exception e) {
        throw (IOException)new IOException().initCause(e);
      }
    }
    return true;
  }

  public int run(String [] argv) throws Exception {
    JobConf job = new JobConf(getConf());
    job.setJarByClass(GenericMRLoadGenerator.class);
    job.setMapperClass(SampleMapper.class);
    job.setReducerClass(SampleReducer.class);
    if (!parseArgs(argv, job)) {
      return -1;
    }

    if (null == FileOutputFormat.getOutputPath(job)) {
      // No output dir? No writes
      job.setOutputFormat(NullOutputFormat.class);
    }

    if (0 == FileInputFormat.getInputPaths(job).length) {
      // No input dir? Generate random data
      System.err.println("No input path; ignoring InputFormat");
      confRandom(job);
    } else if (null != job.getClass("mapred.indirect.input.format", null)) {
      // specified IndirectInputFormat? Build src list
      JobClient jClient = new JobClient(job);
      Path tmpDir = new Path(jClient.getFs().getHomeDirectory(), ".staging");
      Random r = new Random();
      Path indirInputFile = new Path(tmpDir,
          Integer.toString(r.nextInt(Integer.MAX_VALUE), 36) + "_files");
      job.set("mapred.indirect.input.file", indirInputFile.toString());
      SequenceFile.Writer writer = SequenceFile.createWriter(
          tmpDir.getFileSystem(job), job, indirInputFile,
          LongWritable.class, Text.class,
          SequenceFile.CompressionType.NONE);
      try {
        for (Path p : FileInputFormat.getInputPaths(job)) {
          FileSystem fs = p.getFileSystem(job);
          Stack<Path> pathstack = new Stack<Path>();
          pathstack.push(p);
          while (!pathstack.empty()) {
            for (FileStatus stat : fs.listStatus(pathstack.pop())) {
              if (stat.isDir()) {
                if (!stat.getPath().getName().startsWith("_")) {
                  pathstack.push(stat.getPath());
                }
              } else {
                writer.sync();
                writer.append(new LongWritable(stat.getLen()),
                    new Text(stat.getPath().toUri().toString()));
              }
            }
          }
        }
      } finally {
        writer.close();
      }
    }

    Date startTime = new Date();
    System.out.println("Job started: " + startTime);
    JobClient.runJob(job);
    Date endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The job took " +
                       (endTime.getTime() - startTime.getTime()) /1000 +
                       " seconds.");

    return 0;
  }

  /**
   * Main driver/hook into ToolRunner.
   */
  public static void main(String[] argv) throws Exception {
    int res =
      ToolRunner.run(new Configuration(), new GenericMRLoadGenerator(), argv);
    System.exit(res);
  }

  static class RandomInputFormat implements InputFormat {

    public InputSplit[] getSplits(JobConf conf, int numSplits) {
      InputSplit[] splits = new InputSplit[numSplits];
      for (int i = 0; i < numSplits; ++i) {
        splits[i] = new IndirectInputFormat.IndirectSplit(
            new Path("ignore" + i), 1);
      }
      return splits;
    }

    public RecordReader<Text,Text> getRecordReader(InputSplit split,
        JobConf job, Reporter reporter) throws IOException {
      final IndirectInputFormat.IndirectSplit clSplit =
        (IndirectInputFormat.IndirectSplit)split;
      return new RecordReader<Text,Text>() {
        boolean once = true;
        public boolean next(Text key, Text value) {
          if (once) {
            key.set(clSplit.getPath().toString());
            once = false;
            return true;
          }
          return false;
        }
        public Text createKey() { return new Text(); }
        public Text createValue() { return new Text(); }
        public long getPos() { return 0; }
        public void close() { }
        public float getProgress() { return 0.0f; }
      };
    }
  }

  static enum Counters { RECORDS_WRITTEN, BYTES_WRITTEN }

  static class RandomMapOutput extends MapReduceBase
      implements Mapper<Text,Text,Text,Text> {
    StringBuilder sentence = new StringBuilder();
    int keymin;
    int keymax;
    int valmin;
    int valmax;
    long bytesToWrite;
    Random r = new Random();

    private int generateSentence(Text t, int noWords) {
      sentence.setLength(0);
      --noWords;
      for (int i = 0; i < noWords; ++i) {
        sentence.append(words[r.nextInt(words.length)]);
        sentence.append(" ");
      }
      if (noWords >= 0) sentence.append(words[r.nextInt(words.length)]);
      t.set(sentence.toString());
      return sentence.length();
    }

    public void configure(JobConf job) {
      bytesToWrite = job.getLong("test.randomtextwrite.bytes_per_map",
                                    1*1024*1024*1024);
      keymin = job.getInt("test.randomtextwrite.min_words_key", 5);
      keymax = job.getInt("test.randomtextwrite.max_words_key", 10);
      valmin = job.getInt("test.randomtextwrite.min_words_value", 5);
      valmax = job.getInt("test.randomtextwrite.max_words_value", 10);
    }

    public void map(Text key, Text val, OutputCollector<Text,Text> output,
        Reporter reporter) throws IOException {
      long acc = 0L;
      long recs = 0;
      final int keydiff = keymax - keymin;
      final int valdiff = valmax - valmin;
      for (long i = 0L; acc < bytesToWrite; ++i) {
        int recacc = 0;
        recacc += generateSentence(key, keymin +
            (0 == keydiff ? 0 : r.nextInt(keydiff)));
        recacc += generateSentence(val, valmin +
            (0 == valdiff ? 0 : r.nextInt(valdiff)));
        output.collect(key, val);
        ++recs;
        acc += recacc;
        reporter.incrCounter(Counters.BYTES_WRITTEN, recacc);
        reporter.incrCounter(Counters.RECORDS_WRITTEN, 1);
        reporter.setStatus(acc + "/" + (bytesToWrite - acc) + " bytes");
      }
      reporter.setStatus("Wrote " + recs + " records");
    }

  }

  /**
   * When no input dir is specified, generate random data.
   */
  protected static void confRandom(JobConf job)
      throws IOException {
    // from RandomWriter
    job.setInputFormat(RandomInputFormat.class);
    job.setMapperClass(RandomMapOutput.class);

    final ClusterStatus cluster = new JobClient(job).getClusterStatus();
    int numMapsPerHost = job.getInt("test.randomtextwrite.maps_per_host", 10);
    long numBytesToWritePerMap =
      job.getLong("test.randomtextwrite.bytes_per_map", 1*1024*1024*1024);
    if (numBytesToWritePerMap == 0) {
      throw new IOException(
          "Cannot have test.randomtextwrite.bytes_per_map set to 0");
    }
    long totalBytesToWrite = job.getLong("test.randomtextwrite.total_bytes",
         numMapsPerHost * numBytesToWritePerMap * cluster.getTaskTrackers());
    int numMaps = (int)(totalBytesToWrite / numBytesToWritePerMap);
    if (numMaps == 0 && totalBytesToWrite > 0) {
      numMaps = 1;
      job.setLong("test.randomtextwrite.bytes_per_map", totalBytesToWrite);
    }
    job.setNumMapTasks(numMaps);
  }


  // Sampling //

  static abstract class SampleMapReduceBase<K extends WritableComparable,
                                            V extends Writable>
      extends MapReduceBase {

    private long total;
    private long kept = 0;
    private float keep;

    protected void setKeep(float keep) {
      this.keep = keep;
    }

    protected void emit(K key, V val, OutputCollector<K,V> out)
        throws IOException {
      ++total;
      while((float) kept / total < keep) {
        ++kept;
        out.collect(key, val);
      }
    }
  }

  public static class SampleMapper<K extends WritableComparable, V extends Writable>
      extends SampleMapReduceBase<K,V> implements Mapper<K,V,K,V> {

    public void configure(JobConf job) {
      setKeep(job.getFloat("hadoop.sort.map.keep.percent", (float)100.0) /
        (float)100.0);
    }

    public void map(K key, V val,
                    OutputCollector<K,V> output, Reporter reporter)
        throws IOException {
      emit(key, val, output);
    }

  }

  public static class SampleReducer<K extends WritableComparable, V extends Writable>
      extends SampleMapReduceBase<K,V> implements Reducer<K,V,K,V> {

    public void configure(JobConf job) {
      setKeep(job.getFloat("hadoop.sort.reduce.keep.percent", (float)100.0) /
        (float)100.0);
    }

    public void reduce(K key, Iterator<V> values,
                       OutputCollector<K,V> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        emit(key, values.next(), output);
      }
    }

  }

  // Indirect reads //

  /**
   * Obscures the InputFormat and location information to simulate maps
   * reading input from arbitrary locations (&quot;indirect&quot; reads).
   */
  static class IndirectInputFormat implements InputFormat {

    static class IndirectSplit implements InputSplit {
      Path file;
      long len;
      public IndirectSplit() { }
      public IndirectSplit(Path file, long len) {
        this.file = file;
        this.len = len;
      }
      public Path getPath() { return file; }
      public long getLength() { return len; }
      public String[] getLocations() throws IOException {
        return new String[]{};
      }
      public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, file.toString());
        WritableUtils.writeVLong(out, len);
      }
      public void readFields(DataInput in) throws IOException {
        file = new Path(WritableUtils.readString(in));
        len = WritableUtils.readVLong(in);
      }
    }

    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {

      Path src = new Path(job.get("mapred.indirect.input.file", null));
      FileSystem fs = src.getFileSystem(job);

      ArrayList<IndirectSplit> splits = new ArrayList<IndirectSplit>(numSplits);
      LongWritable key = new LongWritable();
      Text value = new Text();
      for (SequenceFile.Reader sl = new SequenceFile.Reader(fs, src, job);
           sl.next(key, value);) {
        splits.add(new IndirectSplit(new Path(value.toString()), key.get()));
      }

      return splits.toArray(new IndirectSplit[splits.size()]);
    }

    public RecordReader getRecordReader(InputSplit split, JobConf job,
        Reporter reporter) throws IOException {
      InputFormat indirIF = (InputFormat)ReflectionUtils.newInstance(
          job.getClass("mapred.indirect.input.format",
            SequenceFileInputFormat.class), job);
      IndirectSplit is = ((IndirectSplit)split);
      return indirIF.getRecordReader(new FileSplit(is.getPath(), 0,
            is.getLength(), (String[])null),
          job, reporter);
    }
  }

  /**
   * A random list of 1000 words from /usr/share/dict/words
   */
  private static final String[] words = {
    "diurnalness", "Homoiousian", "spiranthic", "tetragynian",
    "silverhead", "ungreat", "lithograph", "exploiter",
    "physiologian", "by", "hellbender", "Filipendula",
    "undeterring", "antiscolic", "pentagamist", "hypoid",
    "cacuminal", "sertularian", "schoolmasterism", "nonuple",
    "gallybeggar", "phytonic", "swearingly", "nebular",
    "Confervales", "thermochemically", "characinoid", "cocksuredom",
    "fallacious", "feasibleness", "debromination", "playfellowship",
    "tramplike", "testa", "participatingly", "unaccessible",
    "bromate", "experientialist", "roughcast", "docimastical",
    "choralcelo", "blightbird", "peptonate", "sombreroed",
    "unschematized", "antiabolitionist", "besagne", "mastication",
    "bromic", "sviatonosite", "cattimandoo", "metaphrastical",
    "endotheliomyoma", "hysterolysis", "unfulminated", "Hester",
    "oblongly", "blurredness", "authorling", "chasmy",
    "Scorpaenidae", "toxihaemia", "Dictograph", "Quakerishly",
    "deaf", "timbermonger", "strammel", "Thraupidae",
    "seditious", "plerome", "Arneb", "eristically",
    "serpentinic", "glaumrie", "socioromantic", "apocalypst",
    "tartrous", "Bassaris", "angiolymphoma", "horsefly",
    "kenno", "astronomize", "euphemious", "arsenide",
    "untongued", "parabolicness", "uvanite", "helpless",
    "gemmeous", "stormy", "templar", "erythrodextrin",
    "comism", "interfraternal", "preparative", "parastas",
    "frontoorbital", "Ophiosaurus", "diopside", "serosanguineous",
    "ununiformly", "karyological", "collegian", "allotropic",
    "depravity", "amylogenesis", "reformatory", "epidymides",
    "pleurotropous", "trillium", "dastardliness", "coadvice",
    "embryotic", "benthonic", "pomiferous", "figureheadship",
    "Megaluridae", "Harpa", "frenal", "commotion",
    "abthainry", "cobeliever", "manilla", "spiciferous",
    "nativeness", "obispo", "monilioid", "biopsic",
    "valvula", "enterostomy", "planosubulate", "pterostigma",
    "lifter", "triradiated", "venialness", "tum",
    "archistome", "tautness", "unswanlike", "antivenin",
    "Lentibulariaceae", "Triphora", "angiopathy", "anta",
    "Dawsonia", "becomma", "Yannigan", "winterproof",
    "antalgol", "harr", "underogating", "ineunt",
    "cornberry", "flippantness", "scyphostoma", "approbation",
    "Ghent", "Macraucheniidae", "scabbiness", "unanatomized",
    "photoelasticity", "eurythermal", "enation", "prepavement",
    "flushgate", "subsequentially", "Edo", "antihero",
    "Isokontae", "unforkedness", "porriginous", "daytime",
    "nonexecutive", "trisilicic", "morphiomania", "paranephros",
    "botchedly", "impugnation", "Dodecatheon", "obolus",
    "unburnt", "provedore", "Aktistetae", "superindifference",
    "Alethea", "Joachimite", "cyanophilous", "chorograph",
    "brooky", "figured", "periclitation", "quintette",
    "hondo", "ornithodelphous", "unefficient", "pondside",
    "bogydom", "laurinoxylon", "Shiah", "unharmed",
    "cartful", "noncrystallized", "abusiveness", "cromlech",
    "japanned", "rizzomed", "underskin", "adscendent",
    "allectory", "gelatinousness", "volcano", "uncompromisingly",
    "cubit", "idiotize", "unfurbelowed", "undinted",
    "magnetooptics", "Savitar", "diwata", "ramosopalmate",
    "Pishquow", "tomorn", "apopenptic", "Haversian",
    "Hysterocarpus", "ten", "outhue", "Bertat",
    "mechanist", "asparaginic", "velaric", "tonsure",
    "bubble", "Pyrales", "regardful", "glyphography",
    "calabazilla", "shellworker", "stradametrical", "havoc",
    "theologicopolitical", "sawdust", "diatomaceous", "jajman",
    "temporomastoid", "Serrifera", "Ochnaceae", "aspersor",
    "trailmaking", "Bishareen", "digitule", "octogynous",
    "epididymitis", "smokefarthings", "bacillite", "overcrown",
    "mangonism", "sirrah", "undecorated", "psychofugal",
    "bismuthiferous", "rechar", "Lemuridae", "frameable",
    "thiodiazole", "Scanic", "sportswomanship", "interruptedness",
    "admissory", "osteopaedion", "tingly", "tomorrowness",
    "ethnocracy", "trabecular", "vitally", "fossilism",
    "adz", "metopon", "prefatorial", "expiscate",
    "diathermacy", "chronist", "nigh", "generalizable",
    "hysterogen", "aurothiosulphuric", "whitlowwort", "downthrust",
    "Protestantize", "monander", "Itea", "chronographic",
    "silicize", "Dunlop", "eer", "componental",
    "spot", "pamphlet", "antineuritic", "paradisean",
    "interruptor", "debellator", "overcultured", "Florissant",
    "hyocholic", "pneumatotherapy", "tailoress", "rave",
    "unpeople", "Sebastian", "thermanesthesia", "Coniferae",
    "swacking", "posterishness", "ethmopalatal", "whittle",
    "analgize", "scabbardless", "naught", "symbiogenetically",
    "trip", "parodist", "columniform", "trunnel",
    "yawler", "goodwill", "pseudohalogen", "swangy",
    "cervisial", "mediateness", "genii", "imprescribable",
    "pony", "consumptional", "carposporangial", "poleax",
    "bestill", "subfebrile", "sapphiric", "arrowworm",
    "qualminess", "ultraobscure", "thorite", "Fouquieria",
    "Bermudian", "prescriber", "elemicin", "warlike",
    "semiangle", "rotular", "misthread", "returnability",
    "seraphism", "precostal", "quarried", "Babylonism",
    "sangaree", "seelful", "placatory", "pachydermous",
    "bozal", "galbulus", "spermaphyte", "cumbrousness",
    "pope", "signifier", "Endomycetaceae", "shallowish",
    "sequacity", "periarthritis", "bathysphere", "pentosuria",
    "Dadaism", "spookdom", "Consolamentum", "afterpressure",
    "mutter", "louse", "ovoviviparous", "corbel",
    "metastoma", "biventer", "Hydrangea", "hogmace",
    "seizing", "nonsuppressed", "oratorize", "uncarefully",
    "benzothiofuran", "penult", "balanocele", "macropterous",
    "dishpan", "marten", "absvolt", "jirble",
    "parmelioid", "airfreighter", "acocotl", "archesporial",
    "hypoplastral", "preoral", "quailberry", "cinque",
    "terrestrially", "stroking", "limpet", "moodishness",
    "canicule", "archididascalian", "pompiloid", "overstaid",
    "introducer", "Italical", "Christianopaganism", "prescriptible",
    "subofficer", "danseuse", "cloy", "saguran",
    "frictionlessly", "deindividualization", "Bulanda", "ventricous",
    "subfoliar", "basto", "scapuloradial", "suspend",
    "stiffish", "Sphenodontidae", "eternal", "verbid",
    "mammonish", "upcushion", "barkometer", "concretion",
    "preagitate", "incomprehensible", "tristich", "visceral",
    "hemimelus", "patroller", "stentorophonic", "pinulus",
    "kerykeion", "brutism", "monstership", "merciful",
    "overinstruct", "defensibly", "bettermost", "splenauxe",
    "Mormyrus", "unreprimanded", "taver", "ell",
    "proacquittal", "infestation", "overwoven", "Lincolnlike",
    "chacona", "Tamil", "classificational", "lebensraum",
    "reeveland", "intuition", "Whilkut", "focaloid",
    "Eleusinian", "micromembrane", "byroad", "nonrepetition",
    "bacterioblast", "brag", "ribaldrous", "phytoma",
    "counteralliance", "pelvimetry", "pelf", "relaster",
    "thermoresistant", "aneurism", "molossic", "euphonym",
    "upswell", "ladhood", "phallaceous", "inertly",
    "gunshop", "stereotypography", "laryngic", "refasten",
    "twinling", "oflete", "hepatorrhaphy", "electrotechnics",
    "cockal", "guitarist", "topsail", "Cimmerianism",
    "larklike", "Llandovery", "pyrocatechol", "immatchable",
    "chooser", "metrocratic", "craglike", "quadrennial",
    "nonpoisonous", "undercolored", "knob", "ultratense",
    "balladmonger", "slait", "sialadenitis", "bucketer",
    "magnificently", "unstipulated", "unscourged", "unsupercilious",
    "packsack", "pansophism", "soorkee", "percent",
    "subirrigate", "champer", "metapolitics", "spherulitic",
    "involatile", "metaphonical", "stachyuraceous", "speckedness",
    "bespin", "proboscidiform", "gul", "squit",
    "yeelaman", "peristeropode", "opacousness", "shibuichi",
    "retinize", "yote", "misexposition", "devilwise",
    "pumpkinification", "vinny", "bonze", "glossing",
    "decardinalize", "transcortical", "serphoid", "deepmost",
    "guanajuatite", "wemless", "arval", "lammy",
    "Effie", "Saponaria", "tetrahedral", "prolificy",
    "excerpt", "dunkadoo", "Spencerism", "insatiately",
    "Gilaki", "oratorship", "arduousness", "unbashfulness",
    "Pithecolobium", "unisexuality", "veterinarian", "detractive",
    "liquidity", "acidophile", "proauction", "sural",
    "totaquina", "Vichyite", "uninhabitedness", "allegedly",
    "Gothish", "manny", "Inger", "flutist",
    "ticktick", "Ludgatian", "homotransplant", "orthopedical",
    "diminutively", "monogoneutic", "Kenipsim", "sarcologist",
    "drome", "stronghearted", "Fameuse", "Swaziland",
    "alen", "chilblain", "beatable", "agglomeratic",
    "constitutor", "tendomucoid", "porencephalous", "arteriasis",
    "boser", "tantivy", "rede", "lineamental",
    "uncontradictableness", "homeotypical", "masa", "folious",
    "dosseret", "neurodegenerative", "subtransverse", "Chiasmodontidae",
    "palaeotheriodont", "unstressedly", "chalcites", "piquantness",
    "lampyrine", "Aplacentalia", "projecting", "elastivity",
    "isopelletierin", "bladderwort", "strander", "almud",
    "iniquitously", "theologal", "bugre", "chargeably",
    "imperceptivity", "meriquinoidal", "mesophyte", "divinator",
    "perfunctory", "counterappellant", "synovial", "charioteer",
    "crystallographical", "comprovincial", "infrastapedial", "pleasurehood",
    "inventurous", "ultrasystematic", "subangulated", "supraoesophageal",
    "Vaishnavism", "transude", "chrysochrous", "ungrave",
    "reconciliable", "uninterpleaded", "erlking", "wherefrom",
    "aprosopia", "antiadiaphorist", "metoxazine", "incalculable",
    "umbellic", "predebit", "foursquare", "unimmortal",
    "nonmanufacture", "slangy", "predisputant", "familist",
    "preaffiliate", "friarhood", "corelysis", "zoonitic",
    "halloo", "paunchy", "neuromimesis", "aconitine",
    "hackneyed", "unfeeble", "cubby", "autoschediastical",
    "naprapath", "lyrebird", "inexistency", "leucophoenicite",
    "ferrogoslarite", "reperuse", "uncombable", "tambo",
    "propodiale", "diplomatize", "Russifier", "clanned",
    "corona", "michigan", "nonutilitarian", "transcorporeal",
    "bought", "Cercosporella", "stapedius", "glandularly",
    "pictorially", "weism", "disilane", "rainproof",
    "Caphtor", "scrubbed", "oinomancy", "pseudoxanthine",
    "nonlustrous", "redesertion", "Oryzorictinae", "gala",
    "Mycogone", "reappreciate", "cyanoguanidine", "seeingness",
    "breadwinner", "noreast", "furacious", "epauliere",
    "omniscribent", "Passiflorales", "uninductive", "inductivity",
    "Orbitolina", "Semecarpus", "migrainoid", "steprelationship",
    "phlogisticate", "mesymnion", "sloped", "edificator",
    "beneficent", "culm", "paleornithology", "unurban",
    "throbless", "amplexifoliate", "sesquiquintile", "sapience",
    "astucious", "dithery", "boor", "ambitus",
    "scotching", "uloid", "uncompromisingness", "hoove",
    "waird", "marshiness", "Jerusalem", "mericarp",
    "unevoked", "benzoperoxide", "outguess", "pyxie",
    "hymnic", "euphemize", "mendacity", "erythremia",
    "rosaniline", "unchatteled", "lienteria", "Bushongo",
    "dialoguer", "unrepealably", "rivethead", "antideflation",
    "vinegarish", "manganosiderite", "doubtingness", "ovopyriform",
    "Cephalodiscus", "Muscicapa", "Animalivora", "angina",
    "planispheric", "ipomoein", "cuproiodargyrite", "sandbox",
    "scrat", "Munnopsidae", "shola", "pentafid",
    "overstudiousness", "times", "nonprofession", "appetible",
    "valvulotomy", "goladar", "uniarticular", "oxyterpene",
    "unlapsing", "omega", "trophonema", "seminonflammable",
    "circumzenithal", "starer", "depthwise", "liberatress",
    "unleavened", "unrevolting", "groundneedle", "topline",
    "wandoo", "umangite", "ordinant", "unachievable",
    "oversand", "snare", "avengeful", "unexplicit",
    "mustafina", "sonable", "rehabilitative", "eulogization",
    "papery", "technopsychology", "impressor", "cresylite",
    "entame", "transudatory", "scotale", "pachydermatoid",
    "imaginary", "yeat", "slipped", "stewardship",
    "adatom", "cockstone", "skyshine", "heavenful",
    "comparability", "exprobratory", "dermorhynchous", "parquet",
    "cretaceous", "vesperal", "raphis", "undangered",
    "Glecoma", "engrain", "counteractively", "Zuludom",
    "orchiocatabasis", "Auriculariales", "warriorwise", "extraorganismal",
    "overbuilt", "alveolite", "tetchy", "terrificness",
    "widdle", "unpremonished", "rebilling", "sequestrum",
    "equiconvex", "heliocentricism", "catabaptist", "okonite",
    "propheticism", "helminthagogic", "calycular", "giantly",
    "wingable", "golem", "unprovided", "commandingness",
    "greave", "haply", "doina", "depressingly",
    "subdentate", "impairment", "decidable", "neurotrophic",
    "unpredict", "bicorporeal", "pendulant", "flatman",
    "intrabred", "toplike", "Prosobranchiata", "farrantly",
    "toxoplasmosis", "gorilloid", "dipsomaniacal", "aquiline",
    "atlantite", "ascitic", "perculsive", "prospectiveness",
    "saponaceous", "centrifugalization", "dinical", "infravaginal",
    "beadroll", "affaite", "Helvidian", "tickleproof",
    "abstractionism", "enhedge", "outwealth", "overcontribute",
    "coldfinch", "gymnastic", "Pincian", "Munychian",
    "codisjunct", "quad", "coracomandibular", "phoenicochroite",
    "amender", "selectivity", "putative", "semantician",
    "lophotrichic", "Spatangoidea", "saccharogenic", "inferent",
    "Triconodonta", "arrendation", "sheepskin", "taurocolla",
    "bunghole", "Machiavel", "triakistetrahedral", "dehairer",
    "prezygapophysial", "cylindric", "pneumonalgia", "sleigher",
    "emir", "Socraticism", "licitness", "massedly",
    "instructiveness", "sturdied", "redecrease", "starosta",
    "evictor", "orgiastic", "squdge", "meloplasty",
    "Tsonecan", "repealableness", "swoony", "myesthesia",
    "molecule", "autobiographist", "reciprocation", "refective",
    "unobservantness", "tricae", "ungouged", "floatability",
    "Mesua", "fetlocked", "chordacentrum", "sedentariness",
    "various", "laubanite", "nectopod", "zenick",
    "sequentially", "analgic", "biodynamics", "posttraumatic",
    "nummi", "pyroacetic", "bot", "redescend",
    "dispermy", "undiffusive", "circular", "trillion",
    "Uraniidae", "ploration", "discipular", "potentness",
    "sud", "Hu", "Eryon", "plugger",
    "subdrainage", "jharal", "abscission", "supermarket",
    "countergabion", "glacierist", "lithotresis", "minniebush",
    "zanyism", "eucalypteol", "sterilely", "unrealize",
    "unpatched", "hypochondriacism", "critically", "cheesecutter",
  };
}
