package org.apache.spark.examples;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JavaLogQuery {

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
                return new Tuple3(ip, user, query);
            }
        }
        return new Tuple3(null, null, null);
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
                    "0 73.23.2.15 images.com 1358492557 - Whatup"
    );

    public static final Pattern apacheLogRegex = Pattern.compile(
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");

    public static void main(String[] args) {

            JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaLogQuery"));
            JavaRDD<String> dataSet = (args.length == 1) ? sc.textFile(args[0]) : sc.parallelize(exampleApacheLogs);

            List<Tuple2> result =  dataSet.mapToPair(line -> new Tuple2(extractKey(line), extractStats(line)))
                                    .reduceByKey((a, b) -> ((Stats) a).merge((Stats) b))
                                    .collect();
            result.forEach( n -> {
                        Tuple3 key = (Tuple3<String , String, String>) n._1() ;
                        Stats user = (Stats) n._2();
                        System.out.println( key._2() + "\t" + key._3());
                    });

    }
}
