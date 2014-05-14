package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class JavaPi {


    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("JavaPi"));

        int slice = 2 ;
        if(args[0].length() >0) {
            slice = new Integer(args[1]);
        }
        int n = slice * 100000 ;

        JavaRDD<Integer> arr = sc.parallelize(IntStream.range(0, n).boxed().collect(Collectors.toList()) , slice);

        int count =
                arr.map( i -> {
            float x = (float)Math.random() * 2 - 1 ;
            float y = (float)Math.random() * 2 - 1 ;
            if( x*x + y*y < 1 ) {
                return 1 ;
            }else{
                return 0 ;
            }
        } ).reduce((x,y) -> x+y);
        System.out.println("Pi is roughly "+4.0 * count / n );

    }
}
