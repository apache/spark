/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import com.google.common.collect.Lists;
import scala.Tuple2;
import scala.Tuple3;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes a roll up-style query against Apache logs.
 *  
 * Usage: JavaLogQuery [logFile]
 */
public final class JavaLogQuery {

  public static final List<String> exampleApacheLogs = Lists.newArrayList(
    "10.10.10.10 - \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg " +
      "HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; " +
      "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; " +
      ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR " +
      "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" " +
      "62.24.11.25 images.com 1358492167 - Whatup",
    "10.10.10.10 - \"FRED\" [18/Jan/2013:18:02:37 +1100] \"GET http://images.com/2013/Generic.jpg " +
      "HTTP/1.1\" 304 306 \"http:/referall.com\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; " +
      "GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR " +
      "3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR  " +
      "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.352 \"-\" - \"\" 256 977 988 \"\" " +
      "0 73.23.2.15 images.com 1358492557 - Whatup");

  public static final Pattern apacheLogRegex = Pattern.compile(
    "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");

  /** Tracks the total query count and number of aggregate bytes for a particular group. */
  public static class Stats implements Serializable {

    private final int count;
    private final int numBytes;

    public Stats(int count, int numBytes) {
      this.count = count;
      this.numBytes = numBytes;
    }
    public Stats merge(Stats other) {
      return new Stats(count + other.count, numBytes + other.numBytes);
    }

    public String toString() {
      return String.format("bytes=%s\tn=%s", numBytes, count);
    }
  }

  public static Tuple3<String, String, String> extractKey(String line) {
    Matcher m = apacheLogRegex.matcher(line);
    if (m.find()) {
      String ip = m.group(1);
      String user = m.group(3);
      String query = m.group(5);
      if (!user.equalsIgnoreCase("-")) {
        return new Tuple3<String, String, String>(ip, user, query);
      }
    }
    return new Tuple3<String, String, String>(null, null, null);
  }

  public static Stats extractStats(String line) {
    Matcher m = apacheLogRegex.matcher(line);
    if (m.find()) {
      int bytes = Integer.parseInt(m.group(7));
      return new Stats(1, bytes);
    } else {
      return new Stats(1, 0);
    }
  }

  public static void main(String[] args) {

    SparkConf sparkConf = new SparkConf().setAppName("JavaLogQuery");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    JavaRDD<String> dataSet = (args.length == 1) ? jsc.textFile(args[0]) : jsc.parallelize(exampleApacheLogs);

    JavaPairRDD<Tuple3<String, String, String>, Stats> extracted = dataSet.mapToPair(new PairFunction<String, Tuple3<String, String, String>, Stats>() {
      @Override
      public Tuple2<Tuple3<String, String, String>, Stats> call(String s) {
        return new Tuple2<Tuple3<String, String, String>, Stats>(extractKey(s), extractStats(s));
      }
    });

    JavaPairRDD<Tuple3<String, String, String>, Stats> counts = extracted.reduceByKey(new Function2<Stats, Stats, Stats>() {
      @Override
      public Stats call(Stats stats, Stats stats2) {
        return stats.merge(stats2);
      }
    });

    List<Tuple2<Tuple3<String, String, String>, Stats>> output = counts.collect();
    for (Tuple2<?,?> t : output) {
      System.out.println(t._1() + "\t" + t._2());
    }
    jsc.stop();
  }
}
