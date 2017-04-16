/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.io.file.tfile;

import java.io.IOException;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG;
import org.apache.hadoop.io.file.tfile.TFile.Reader;
import org.apache.hadoop.io.file.tfile.TFile.Writer;
import org.apache.hadoop.io.file.tfile.TFile.Reader.Scanner;

/**
 * test the performance for seek.
 *
 */
public class TestTFileSeek extends TestCase { 
  private MyOptions options;
  private Configuration conf;
  private Path path;
  private FileSystem fs;
  private NanoTimer timer;
  private Random rng;
  private DiscreteRNG keyLenGen;
  private KVGenerator kvGen;

  @Override
  public void setUp() throws IOException {
    if (options == null) {
      options = new MyOptions(new String[0]);
    }

    conf = new Configuration();
    conf.setInt("tfile.fs.input.buffer.size", options.fsInputBufferSize);
    conf.setInt("tfile.fs.output.buffer.size", options.fsOutputBufferSize);
    path = new Path(new Path(options.rootDir), options.file);
    fs = path.getFileSystem(conf);
    timer = new NanoTimer(false);
    rng = new Random(options.seed);
    keyLenGen =
        new RandomDistribution.Zipf(new Random(rng.nextLong()),
            options.minKeyLen, options.maxKeyLen, 1.2);
    DiscreteRNG valLenGen =
        new RandomDistribution.Flat(new Random(rng.nextLong()),
            options.minValLength, options.maxValLength);
    DiscreteRNG wordLenGen =
        new RandomDistribution.Flat(new Random(rng.nextLong()),
            options.minWordLen, options.maxWordLen);
    kvGen =
        new KVGenerator(rng, true, keyLenGen, valLenGen, wordLenGen,
            options.dictSize);
  }
  
  @Override
  public void tearDown() throws IOException {
    fs.delete(path, true);
  }
  
  private static FSDataOutputStream createFSOutput(Path name, FileSystem fs)
    throws IOException {
    if (fs.exists(name)) {
      fs.delete(name, true);
    }
    FSDataOutputStream fout = fs.create(name);
    return fout;
  }

  private void createTFile() throws IOException {
    long totalBytes = 0;
    FSDataOutputStream fout = createFSOutput(path, fs);
    try {
      Writer writer =
          new Writer(fout, options.minBlockSize, options.compress, "memcmp",
              conf);
      try {
        BytesWritable key = new BytesWritable();
        BytesWritable val = new BytesWritable();
        timer.start();
        for (long i = 0; true; ++i) {
          if (i % 1000 == 0) { // test the size for every 1000 rows.
            if (fs.getFileStatus(path).getLen() >= options.fileSize) {
              break;
            }
          }
          kvGen.next(key, val, false);
          writer.append(key.get(), 0, key.getSize(), val.get(), 0, val
              .getSize());
          totalBytes += key.getSize();
          totalBytes += val.getSize();
        }
        timer.stop();
      }
      finally {
        writer.close();
      }
    }
    finally {
      fout.close();
    }
    double duration = (double)timer.read()/1000; // in us.
    long fsize = fs.getFileStatus(path).getLen();

    System.out.printf(
        "time: %s...uncompressed: %.2fMB...raw thrpt: %.2fMB/s\n",
        timer.toString(), (double) totalBytes / 1024 / 1024, totalBytes
            / duration);
    System.out.printf("time: %s...file size: %.2fMB...disk thrpt: %.2fMB/s\n",
        timer.toString(), (double) fsize / 1024 / 1024, fsize / duration);
  }
  
  public void seekTFile() throws IOException {
    int miss = 0;
    long totalBytes = 0;
    FSDataInputStream fsdis = fs.open(path);
    Reader reader =
      new Reader(fsdis, fs.getFileStatus(path).getLen(), conf);
    KeySampler kSampler =
        new KeySampler(rng, reader.getFirstKey(), reader.getLastKey(),
            keyLenGen);
    Scanner scanner = reader.createScanner();
    BytesWritable key = new BytesWritable();
    BytesWritable val = new BytesWritable();
    timer.reset();
    timer.start();
    for (int i = 0; i < options.seekCount; ++i) {
      kSampler.next(key);
      scanner.lowerBound(key.get(), 0, key.getSize());
      if (!scanner.atEnd()) {
        scanner.entry().get(key, val);
        totalBytes += key.getSize();
        totalBytes += val.getSize();
      }
      else {
        ++miss;
      }
    }
    timer.stop();
    double duration = (double) timer.read() / 1000; // in us.
    System.out.printf(
        "time: %s...avg seek: %s...%d hit...%d miss...avg I/O size: %.2fKB\n",
        timer.toString(), NanoTimer.nanoTimeToString(timer.read()
            / options.seekCount), options.seekCount - miss, miss,
        (double) totalBytes / 1024 / (options.seekCount - miss));

  }
  
  public void testSeeks() throws IOException {
    String[] supported = TFile.getSupportedCompressionAlgorithms();
    boolean proceed = false;
    for (String c : supported) {
      if (c.equals(options.compress)) {
        proceed = true;
        break;
      }
    }

    if (!proceed) {
      System.out.println("Skipped for " + options.compress);
      return;
    }

    if (options.doCreate()) {
      createTFile();
    }

    if (options.doRead()) {
      seekTFile();
    }
  }
  
  private static class IntegerRange {
    private final int from, to;

    public IntegerRange(int from, int to) {
      this.from = from;
      this.to = to;
    }

    public static IntegerRange parse(String s) throws ParseException {
      StringTokenizer st = new StringTokenizer(s, " \t,");
      if (st.countTokens() != 2) {
        throw new ParseException("Bad integer specification: " + s);
      }
      int from = Integer.parseInt(st.nextToken());
      int to = Integer.parseInt(st.nextToken());
      return new IntegerRange(from, to);
    }

    public int from() {
      return from;
    }

    public int to() {
      return to;
    }
  }

  private static class MyOptions {
    // hard coded constants
    int dictSize = 1000;
    int minWordLen = 5;
    int maxWordLen = 20;
    int osInputBufferSize = 64 * 1024;
    int osOutputBufferSize = 64 * 1024;
    int fsInputBufferSizeNone = 0;
    int fsInputBufferSizeLzo = 0;
    int fsInputBufferSizeGz = 0;
    int fsOutputBufferSizeNone = 1;
    int fsOutputBufferSizeLzo = 1;
    int fsOutputBufferSizeGz = 1;
   
    String rootDir =
        System.getProperty("test.build.data", "/tmp/tfile-test");
    String file = "TestTFileSeek";
    String compress = "gz";
    int minKeyLen = 10;
    int maxKeyLen = 50;
    int minValLength = 100;
    int maxValLength = 200;
    int minBlockSize = 64 * 1024;
    int fsOutputBufferSize = 1;
    int fsInputBufferSize = 0;
    long fileSize = 3 * 1024 * 1024;
    long seekCount = 1000;
    long seed;

    static final int OP_CREATE = 1;
    static final int OP_READ = 2;
    int op = OP_CREATE | OP_READ;

    boolean proceed = false;

    public MyOptions(String[] args) {
      seed = System.nanoTime();

      try {
        Options opts = buildOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine line = parser.parse(opts, args, true);
        processOptions(line, opts);
        validateOptions();
      }
      catch (ParseException e) {
        System.out.println(e.getMessage());
        System.out.println("Try \"--help\" option for details.");
        setStopProceed();
      }
    }

    public boolean proceed() {
      return proceed;
    }

    private Options buildOptions() {
      Option compress =
          OptionBuilder.withLongOpt("compress").withArgName("[none|lzo|gz]")
              .hasArg().withDescription("compression scheme").create('c');

      Option fileSize =
          OptionBuilder.withLongOpt("file-size").withArgName("size-in-MB")
              .hasArg().withDescription("target size of the file (in MB).")
              .create('s');

      Option fsInputBufferSz =
          OptionBuilder.withLongOpt("fs-input-buffer").withArgName("size")
              .hasArg().withDescription(
                  "size of the file system input buffer (in bytes).").create(
                  'i');

      Option fsOutputBufferSize =
          OptionBuilder.withLongOpt("fs-output-buffer").withArgName("size")
              .hasArg().withDescription(
                  "size of the file system output buffer (in bytes).").create(
                  'o');

      Option keyLen =
          OptionBuilder
              .withLongOpt("key-length")
              .withArgName("min,max")
              .hasArg()
              .withDescription(
                  "the length range of the key (in bytes)")
              .create('k');

      Option valueLen =
          OptionBuilder
              .withLongOpt("value-length")
              .withArgName("min,max")
              .hasArg()
              .withDescription(
                  "the length range of the value (in bytes)")
              .create('v');

      Option blockSz =
          OptionBuilder.withLongOpt("block").withArgName("size-in-KB").hasArg()
              .withDescription("minimum block size (in KB)").create('b');

      Option seed =
          OptionBuilder.withLongOpt("seed").withArgName("long-int").hasArg()
              .withDescription("specify the seed").create('S');

      Option operation =
          OptionBuilder.withLongOpt("operation").withArgName("r|w|rw").hasArg()
              .withDescription(
                  "action: seek-only, create-only, seek-after-create").create(
                  'x');

      Option rootDir =
          OptionBuilder.withLongOpt("root-dir").withArgName("path").hasArg()
              .withDescription(
                  "specify root directory where files will be created.")
              .create('r');

      Option file =
          OptionBuilder.withLongOpt("file").withArgName("name").hasArg()
              .withDescription("specify the file name to be created or read.")
              .create('f');

      Option seekCount =
          OptionBuilder
              .withLongOpt("seek")
              .withArgName("count")
              .hasArg()
              .withDescription(
                  "specify how many seek operations we perform (requires -x r or -x rw.")
              .create('n');

      Option help =
          OptionBuilder.withLongOpt("help").hasArg(false).withDescription(
              "show this screen").create("h");

      return new Options().addOption(compress).addOption(fileSize).addOption(
          fsInputBufferSz).addOption(fsOutputBufferSize).addOption(keyLen)
          .addOption(blockSz).addOption(rootDir).addOption(valueLen).addOption(
              operation).addOption(seekCount).addOption(file).addOption(help);

    }

    private void processOptions(CommandLine line, Options opts)
        throws ParseException {
      // --help -h and --version -V must be processed first.
      if (line.hasOption('h')) {
        HelpFormatter formatter = new HelpFormatter();
        System.out.println("TFile and SeqFile benchmark.");
        System.out.println();
        formatter.printHelp(100,
            "java ... TestTFileSeqFileComparison [options]",
            "\nSupported options:", opts, "");
        return;
      }

      if (line.hasOption('c')) {
        compress = line.getOptionValue('c');
      }

      if (line.hasOption('d')) {
        dictSize = Integer.parseInt(line.getOptionValue('d'));
      }

      if (line.hasOption('s')) {
        fileSize = Long.parseLong(line.getOptionValue('s')) * 1024 * 1024;
      }

      if (line.hasOption('i')) {
        fsInputBufferSize = Integer.parseInt(line.getOptionValue('i'));
      }

      if (line.hasOption('o')) {
        fsOutputBufferSize = Integer.parseInt(line.getOptionValue('o'));
      }
      
      if (line.hasOption('n')) {
        seekCount = Integer.parseInt(line.getOptionValue('n'));
      }

      if (line.hasOption('k')) {
        IntegerRange ir = IntegerRange.parse(line.getOptionValue('k'));
        minKeyLen = ir.from();
        maxKeyLen = ir.to();
      }

      if (line.hasOption('v')) {
        IntegerRange ir = IntegerRange.parse(line.getOptionValue('v'));
        minValLength = ir.from();
        maxValLength = ir.to();
      }

      if (line.hasOption('b')) {
        minBlockSize = Integer.parseInt(line.getOptionValue('b')) * 1024;
      }

      if (line.hasOption('r')) {
        rootDir = line.getOptionValue('r');
      }
      
      if (line.hasOption('f')) {
        file = line.getOptionValue('f');
      }

      if (line.hasOption('S')) {
        seed = Long.parseLong(line.getOptionValue('S'));
      }

      if (line.hasOption('x')) {
        String strOp = line.getOptionValue('x');
        if (strOp.equals("r")) {
          op = OP_READ;
        }
        else if (strOp.equals("w")) {
          op = OP_CREATE;
        }
        else if (strOp.equals("rw")) {
          op = OP_CREATE | OP_READ;
        }
        else {
          throw new ParseException("Unknown action specifier: " + strOp);
        }
      }

      proceed = true;
    }

    private void validateOptions() throws ParseException {
      if (!compress.equals("none") && !compress.equals("lzo")
          && !compress.equals("gz")) {
        throw new ParseException("Unknown compression scheme: " + compress);
      }

      if (minKeyLen >= maxKeyLen) {
        throw new ParseException(
            "Max key length must be greater than min key length.");
      }

      if (minValLength >= maxValLength) {
        throw new ParseException(
            "Max value length must be greater than min value length.");
      }

      if (minWordLen >= maxWordLen) {
        throw new ParseException(
            "Max word length must be greater than min word length.");
      }
      return;
    }

    private void setStopProceed() {
      proceed = false;
    }

    public boolean doCreate() {
      return (op & OP_CREATE) != 0;
    }

    public boolean doRead() {
      return (op & OP_READ) != 0;
    }
  }
  
  public static void main(String[] argv) throws IOException {
    TestTFileSeek testCase = new TestTFileSeek();
    MyOptions options = new MyOptions(argv);
    
    if (options.proceed == false) {
      return;
    }

    testCase.options = options;
    testCase.setUp();
    testCase.testSeeks();
    testCase.tearDown();
  }
}
