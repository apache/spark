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

package org.apache.hadoop.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileBench extends Configured implements Tool {

  static int printUsage() {
    ToolRunner.printGenericCommandUsage(System.out);
    System.out.println(
"Usage: Task list:           -[no]r -[no]w\n" +
"       Format:              -[no]seq -[no]txt\n" +
"       CompressionCodec:    -[no]zip -[no]pln\n" +
"       CompressionType:     -[no]blk -[no]rec\n" +
"       Required:            -dir <working dir>\n" +
"All valid combinations are implicitly enabled, unless an option is enabled\n" +
"explicitly. For example, specifying \"-zip\", excludes -pln,\n" +
"unless they are also explicitly included, as in \"-pln -zip\"\n" +
"Note that CompressionType params only apply to SequenceFiles\n\n" +
"Useful options to set:\n" +
"-D fs.default.name=\"file:///\" \\\n" +
"-D fs.file.impl=org.apache.hadoop.fs.RawLocalFileSystem \\\n" +
"-D filebench.file.bytes=$((10*1024*1024*1024)) \\\n" +
"-D filebench.key.words=5 \\\n" +
"-D filebench.val.words=20\n");
    return -1;
  }

  static String[] keys;
  static String[] values;
  static StringBuilder sentence = new StringBuilder();

  private static String generateSentence(Random r, int noWords) {
    sentence.setLength(0);
    for (int i=0; i < noWords; ++i) {
      sentence.append(words[r.nextInt(words.length)]);
      sentence.append(" ");
    }
    return sentence.toString();
  }

  // fill keys, values with ~1.5 blocks for block-compressed seq fill
  private static void fillBlocks(JobConf conf) {
    Random r = new Random();
    long seed = conf.getLong("filebench.seed", -1);
    if (seed > 0) {
      r.setSeed(seed);
    }

    int keylen = conf.getInt("filebench.key.words", 5);
    int vallen = conf.getInt("filebench.val.words", 20);
    int acc = (3 * conf.getInt("io.seqfile.compress.blocksize", 1000000)) >> 1;
    ArrayList<String> k = new ArrayList<String>();
    ArrayList<String> v = new ArrayList<String>();
    for (int i = 0; acc > 0; ++i) {
      String s = generateSentence(r, keylen);
      acc -= s.length();
      k.add(s);
      s = generateSentence(r, vallen);
      acc -= s.length();
      v.add(s);
    }
    keys = k.toArray(new String[0]);
    values = v.toArray(new String[0]);
  }

  @SuppressWarnings("unchecked") // OutputFormat instantiation
  static long writeBench(JobConf conf) throws IOException {
    long filelen = conf.getLong("filebench.file.bytes", 5 * 1024 * 1024 * 1024);
    Text key = new Text();
    Text val = new Text();

    final String fn = conf.get("test.filebench.name", "");
    final Path outd = FileOutputFormat.getOutputPath(conf);
    conf.set("mapred.work.output.dir", outd.toString());
    OutputFormat outf = conf.getOutputFormat();
    RecordWriter<Text,Text> rw =
      outf.getRecordWriter(outd.getFileSystem(conf), conf, fn,
                           Reporter.NULL);
    try {
      long acc = 0L;
      Date start = new Date();
      for (int i = 0; acc < filelen; ++i) {
        i %= keys.length;
        key.set(keys[i]);
        val.set(values[i]);
        rw.write(key, val);
        acc += keys[i].length();
        acc += values[i].length();
      }
      Date end = new Date();
      return end.getTime() - start.getTime();
    } finally {
      rw.close(Reporter.NULL);
    }
  }

  @SuppressWarnings("unchecked") // InputFormat instantiation
  static long readBench(JobConf conf) throws IOException {
    InputFormat inf = conf.getInputFormat();
    final String fn = conf.get("test.filebench.name", "");
    Path pin = new Path(FileInputFormat.getInputPaths(conf)[0], fn);
    FileStatus in = pin.getFileSystem(conf).getFileStatus(pin);
    RecordReader rr = inf.getRecordReader(new FileSplit(pin, 0, in.getLen(), 
                                          (String[])null), conf, Reporter.NULL);
    try {
      Object key = rr.createKey();
      Object val = rr.createValue();
      Date start = new Date();
      while (rr.next(key, val));
      Date end = new Date();
      return end.getTime() - start.getTime();
    } finally {
      rr.close();
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FileBench(), args);
    System.exit(res);
  }

  /**
   * Process params from command line and run set of benchmarks specified.
   */
  public int run(String[] argv) throws IOException {
    JobConf job = new JobConf(getConf());
    EnumSet<CCodec> cc = null;
    EnumSet<CType> ct = null;
    EnumSet<Format> f = null;
    EnumSet<RW> rw = null;
    Path root = null;
    FileSystem fs = FileSystem.get(job);
    for(int i = 0; i < argv.length; ++i) {
      try {
        if ("-dir".equals(argv[i])) {
          root = new Path(argv[++i]).makeQualified(fs);
          System.out.println("DIR: " + root.toString());
        } else if ("-seed".equals(argv[i])) {
          job.setLong("filebench.seed", Long.valueOf(argv[++i]));
        } else if (argv[i].startsWith("-no")) {
          String arg = argv[i].substring(3);
          cc = rem(CCodec.class, cc, arg);
          ct = rem(CType.class, ct, arg);
          f =  rem(Format.class, f, arg);
          rw = rem(RW.class, rw, arg);
        } else {
          String arg = argv[i].substring(1);
          cc = add(CCodec.class, cc, arg);
          ct = add(CType.class, ct, arg);
          f =  add(Format.class, f, arg);
          rw = add(RW.class, rw, arg);
        }
      } catch (Exception e) {
        throw (IOException)new IOException().initCause(e);
      }
    }
    if (null == root) {
      System.out.println("Missing -dir param");
      printUsage();
      return -1;
    }

    fillBlocks(job);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(job, root);
    FileOutputFormat.setOutputPath(job, root);

    if (null == cc) cc = EnumSet.allOf(CCodec.class);
    if (null == ct) ct = EnumSet.allOf(CType.class);
    if (null == f)  f  = EnumSet.allOf(Format.class);
    if (null == rw) rw = EnumSet.allOf(RW.class);
    for (RW rwop : rw) {
      for (Format fmt : f) {
        fmt.configure(job);
        for (CCodec cod : cc) {
          cod.configure(job);
          if (!(fmt == Format.txt || cod == CCodec.pln)) {
            for (CType typ : ct) {
              String fn =
                fmt.name().toUpperCase() + "_" +
                cod.name().toUpperCase() + "_" +
                typ.name().toUpperCase();
              typ.configure(job);
              System.out.print(rwop.name().toUpperCase() + " " + fn + ": ");
              System.out.println(rwop.exec(fn, job) / 1000 +
                  " seconds");
            }
          } else {
            String fn =
              fmt.name().toUpperCase() + "_" +
              cod.name().toUpperCase();
            Path p = new Path(root, fn);
            if (rwop == RW.r && !fs.exists(p)) {
              fn += cod.getExt();
            }
            System.out.print(rwop.name().toUpperCase() + " " + fn + ": ");
            System.out.println(rwop.exec(fn, job) / 1000 +
                " seconds");
          }
        }
      }
    }
    return 0;
  }

  // overwrought argument processing and wordlist follow
  enum CCodec {
    zip(GzipCodec.class, ".gz"), pln(null, "");

    Class<? extends CompressionCodec> inf;
    String ext;
    CCodec(Class<? extends CompressionCodec> inf, String ext) {
      this.inf = inf;
      this.ext = ext;
    }
    public void configure(JobConf job) {
      if (inf != null) {
        job.setBoolean("mapred.output.compress", true);
        job.setClass("mapred.output.compression.codec", inf,
            CompressionCodec.class);
      } else {
        job.setBoolean("mapred.output.compress", false);
      }
    }
    public String getExt() { return ext; }
  }
  enum CType {
    blk("BLOCK"),
    rec("RECORD");

    String typ;
    CType(String typ) { this.typ = typ; }
    public void configure(JobConf job) {
      job.set("mapred.map.output.compression.type", typ);
      job.set("mapred.output.compression.type", typ);
    }
  }
  enum Format {
    seq(SequenceFileInputFormat.class, SequenceFileOutputFormat.class),
    txt(TextInputFormat.class, TextOutputFormat.class);

    Class<? extends InputFormat> inf;
    Class<? extends OutputFormat> of;
    Format(Class<? extends InputFormat> inf, Class<? extends OutputFormat> of) {
      this.inf = inf;
      this.of = of;
    }
    public void configure(JobConf job) {
      if (null != inf) job.setInputFormat(inf);
      if (null != of) job.setOutputFormat(of);
    }
  }
  enum RW {
    w() {
      public long exec(String fn, JobConf job) throws IOException {
        job.set("test.filebench.name", fn);
        return writeBench(job);
      }
    },

    r() {
      public long exec(String fn, JobConf job) throws IOException {
        job.set("test.filebench.name", fn);
        return readBench(job);
      }
    };

    public abstract long exec(String fn, JobConf job) throws IOException;
  }
  static Map<Class<? extends Enum>, Map<String,? extends Enum>> fullmap
    = new HashMap<Class<? extends Enum>, Map<String,? extends Enum>>();
  static {
    // can't effectively use Enum::valueOf
    Map<String,CCodec> m1 = new HashMap<String,CCodec>();
    for (CCodec v : CCodec.values()) m1.put(v.name(), v);
    fullmap.put(CCodec.class, m1);
    Map<String,CType> m2 = new HashMap<String,CType>();
    for (CType v : CType.values()) m2.put(v.name(), v);
    fullmap.put(CType.class, m2);
    Map<String,Format> m3 = new HashMap<String,Format>();
    for (Format v : Format.values()) m3.put(v.name(), v);
    fullmap.put(Format.class, m3);
    Map<String,RW> m4 = new HashMap<String,RW>();
    for (RW v : RW.values()) m4.put(v.name(), v);
    fullmap.put(RW.class, m4);
  }

  public static <T extends Enum<T>> EnumSet<T> rem(Class<T> c,
      EnumSet<T> set, String s) {
    if (null != fullmap.get(c) && fullmap.get(c).get(s) != null) {
      if (null == set) {
        set = EnumSet.allOf(c);
      }
      set.remove(fullmap.get(c).get(s));
    }
    return set;
  }

  @SuppressWarnings("unchecked")
  public static <T extends Enum<T>> EnumSet<T> add(Class<T> c,
      EnumSet<T> set, String s) {
    if (null != fullmap.get(c) && fullmap.get(c).get(s) != null) {
      if (null == set) {
        set = EnumSet.noneOf(c);
      }
      set.add((T)fullmap.get(c).get(s));
    }
    return set;
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
