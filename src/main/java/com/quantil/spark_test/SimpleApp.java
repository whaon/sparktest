package com.quantil.spark_test;
/* SimpleApp.java */

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class SimpleApp {
  
  static Logger log = LoggerFactory.getLogger(SimpleApp.class);
  
  private static final Pattern SPACE = Pattern.compile(" ");

  
  public static void main(String[] args) throws InterruptedException {
    //String logFile = "/usr/local/fluentd/spark-2.2.1-bin-hadoop2.7/README.md"; // Should be some file on your system
    //String logFile = "E:\\E\\java\\spark\\spark-2.2.1-bin-hadoop2.7/README.md"; // Should be some file on your system
    String logFile = "/usr/local/fluentd/th.txt"; // Should be some file on your system
   /* SparkSession spark = SparkSession.builder().appName("Simple Application")
        //.config("spark.master", "spark://220.243.205.189:7077")
        //.master("local")
        .getOrCreate();*/
        //JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
    
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount").setMaster("local");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    jsc.setLogLevel("warn");
    
    
    List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 6);  
    
    
    JavaRDD<Integer> rdd = jsc.parallelize(data);
    
    rdd.take(2).forEach(s -> System.out.println(s));
    
    System.out.println(rdd.filter(s -> s > 3).count());
    
    List<Integer> tmp = rdd.filter(s -> s > 3).collect();
    System.out.println(tmp);
    System.out.println(rdd.filter(s -> s > 3).top(2));
    
    // 这里的map是映射的意思
    System.out.println(rdd.filter(s -> s > 3).map(new Function<Integer, String>() {

      @Override
      public String call(Integer v1) throws Exception {
        return v1 + "a";
      }
    }).collect());
    
 // 这里的map是映射的意思，和map，差不多，只不过返回的是Iterator
    System.out.println(rdd.filter(s -> s > 3).flatMap(new FlatMapFunction<Integer, String>() {

      @Override
      public Iterator<String> call(Integer t) throws Exception {
        return Arrays.asList(t + "t", "ab").iterator();
      }
    }).collect());
    
    // 映射成一个pair对象(Tuple2)
    System.out.println(rdd.filter(s -> s > 3).mapToPair(new PairFunction<Integer, Integer, String>() {

      @Override
      public Tuple2<Integer, String> call(Integer t) throws Exception {
        return new Tuple2(t , t + "a");
      }
    }).collect());
    
    System.out.println(rdd.filter(s -> s > 3).mapToPair(new PairFunction<Integer, Integer, String>() {

      @Override
      public Tuple2<Integer, String> call(Integer t) throws Exception {
        return new Tuple2(t , t + "a");
      }// reduceByKey,即根据相同的key进行操作
    }).reduceByKey(new Function2<String, String, String>() {
      
      @Override
      public String call(String v1, String v2) throws Exception {
        return v1 + v2;
      }
    }).collect());    
    
    System.out.println(rdd.filter(s -> s > 3).mapToPair(new PairFunction<Integer, Integer, String>() {

      @Override
      public Tuple2<Integer, String> call(Integer t) throws Exception {
        return new Tuple2(t , t + "a");
      }// reduce,参数是两个Tuple2,自己定义逻辑
    }).reduce(new Function2<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple2<Integer,String>>() {

      @Override
      public Tuple2<Integer, String> call(Tuple2<Integer, String> v1, Tuple2<Integer, String> v2) throws Exception {
        if(v1._1() > v2._1) {
          return v1;
        }
        return v2;
      }
    }));
    
    // reduce操作，数据的个数会减少，而group操作，数据的个数不变，只是分组了而已
    System.out.println(rdd.filter(s -> s > 3).flatMap(new FlatMapFunction<Integer, String>() {

      @Override
      public Iterator<String> call(Integer t) throws Exception {
        return Arrays.asList(t + "t", "ab").iterator();
      }// 分组的规则，即根据什么来分组
    }).groupBy(new Function<String, Integer>() {

      @Override
      public Integer call(String v1) throws Exception {
        return v1.length();
      }
    }).collect());
    
    log.info("hahaha");
    //log.info(args[0]);
    
    List<Tuple2<String, Integer>> scoreList = Arrays.asList(  
        new Tuple2<String, Integer>("class1", 80),  
        new Tuple2<String, Integer>("class2", 90),  
        new Tuple2<String, Integer>("class1", 97),  
        new Tuple2<String, Integer>("class2", 89));  
  
    JavaPairRDD<String, Integer> scores = jsc.parallelizePairs(scoreList);  
    
    JavaPairRDD<String, Iterable<Integer>> groupedScores  = scores.groupByKey();
    
    System.out.println(groupedScores.collect());
    
    groupedScores.foreach(f -> System.out.println(f._2().toString()));
    
    JavaPairRDD<String, Integer> ff = scores.reduceByKey((a,b) -> (a + b));
    
    ff.foreach(f -> System.out.println(f));
 
    
    /*Dataset<String> logData = spark.read().textFile(logFile).cache();

    long numAs = logData.filter(s -> s.contains("a")).count();
    long numBs = logData.filter(s -> s.contains("b")).count();

    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);*/

    //spark.stop();
    
  }
}