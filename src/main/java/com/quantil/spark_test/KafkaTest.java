package com.quantil.spark_test;
/* SimpleApp.java */

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class KafkaTest {
  
  static Logger log = LoggerFactory.getLogger(KafkaTest.class);
  
  private static final Pattern SPACE = Pattern.compile(" ");

  
  public static void main(String[] args) throws InterruptedException {
    Map<String, Object> kafkaParams = new HashMap<>();
    //kafkaParams.put("bootstrap.servers", "220.243.234.197:19092,220.243.234.198:19092");// DAL
    kafkaParams.put("bootstrap.servers", " 220.243.205.191:19092,220.243.205.192:19092");// NY
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "mytest");
    //kafkaParams.put("startingOffsets", "earliest");
    kafkaParams.put("auto.offset.reset", "earliest");
    kafkaParams.put("heartbeat.interval.ms", "55000");
    kafkaParams.put("request.timeout.ms", "65000");
    kafkaParams.put("session.timeout.ms", "60000");
    kafkaParams.put("enable.auto.commit", false);// 这个意思是不记录offset，所以重启后，会从上次有记录的地方重新开始消费,而忽略auto.offset.reset
    kafkaParams.put("receive.buffer.bytes", 655350000);
    
    /* enable.auto.commit为false，不记录offest，忽略auto.offset.reset参数，每次都从上次残留的offset记录
     * enable.auto.commit为true，则记录offset，如果为earliest，则从上次残留的offset记录
     * enable.auto.commit为false，为latest，则从latest开始
     * 
     * 
     */

    Collection<String> topics = Arrays.asList("qosslog");
    
    //kafkaParams.put("spark.streaming.blockInterval", 50000);
    //kafkaParams.put("spark.default.parallelism", 64);
    
    // 多种构造方法，可以可以设置日志级别，和修改log4j配置文件一样
    SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");//.set("spark.streaming.blockInterval", "50000");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    sc.setLogLevel("WARN");
    //sc.setCheckpointDir("/usr/local/checkpoint");
    
    JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.milliseconds(2000));
    
    JavaInputDStream<ConsumerRecord<String, String>> stream =
      KafkaUtils.createDirectStream(
        streamingContext,
        LocationStrategies.PreferConsistent(),
        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
      );
    
    // 开始的线程数会和kafka的分区数保持一致，增加分区数，可以提升效率
    JavaPairDStream<String, Integer>  tmp = stream.map(x -> {
      //System.out.println("map" + AddressUtils.getHostName());
      return JsonUtil.fromJson(x.value(), Map.class);
    })
      .mapToPair(m ->  {
      //System.out.println("mapPair" + AddressUtils.getHostName());
      return new Tuple2<String, Integer>(RandomStringUtils.randomAlphabetic(2), 1);
      })
      //.repartition(64)
      .reduceByKey((x,y) ->  {
        //System.out.println("reduceByKey" + AddressUtils.getHostName());
        return (x+y);});
    
/*    JavaPairDStream<String, Integer>  tmp = stream.cache().map(x -> {
      //System.out.println("map" + AddressUtils.getHostName());
      return JsonUtil.fromJson(x.value(), Map.class);
    })
        .mapToPair(m ->  {
          //System.out.println("mapPair" + AddressUtils.getHostName());
          return new Tuple2<String, Integer>(RandomStringUtils.randomAlphabetic(2), 1);
        })//.repartition(64)
        .reduceByKey((x,y) ->  {
          //System.out.println("reduceByKey" + AddressUtils.getHostName());
          return (x+y);});
    
*/    
    //tmp.checkpoint(Durations.seconds(5));
    
    /*stream.foreachRDD(c -> {
      System.out.println(c.collect().size());
    });*/
    
    // collect方法很耗内存，如果对象很多的花
    
    // 必须要有一个action操作
    //tmp.foreachRDD(x -> x.foreach(y -> System.out.println(y._1 + "---" + y._2)));
    tmp.foreachRDD(x -> x.foreachPartition(f -> {
      while(f.hasNext()) {
        System.out.println(f.next());
      }
      
    }));
    
    //tmp.print(); 
  //stream.count().print();
    
   /* OffsetRange[] offsetRanges = {
        // topic, partition, inclusive starting offset, exclusive ending offset
        OffsetRange.create("qosslog", 0, 0, 0),
        OffsetRange.create("qosslog", 1, 0, 0)
      };

      JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
          sc,
        kafkaParams,
        offsetRanges,
        LocationStrategies.PreferConsistent()
      );
      
      
      System.out.println(
    rdd.map(x -> JsonUtil.fromJson(x.value(), Map.class)).mapToPair(m ->  new Tuple2<String, Integer>(m.get("pubIp").toString(), 1))
    .collect());*/
      
      
    /*counts.foreachRDD(t -> {
      System.out.println("$" + t.keys().collect());
      System.out.println(AddressUtils.getHostName());
      });*/
    
    
    System.out.println(AddressUtils.getHostName());
    
    /*stream.mapToPair(
        record -> new Tuple2<>(record.key(), record.value())
        )
    
    ;
    jp.foreachRDD(s -> System.out.println(s.values()));*/
    
    /*JavaPairDStream<String, String> jp2 = stream.mapToPair(new PairFunction<ConsumerRecord<String,String>, String, String>() {

      @Override
      public Tuple2<String, String> call(ConsumerRecord<String, String> t) throws Exception {
        System.out.println(t.value());
        return new Tuple2<>(t.key(), t.value());
      }
    });
    
    //jp2.print();
    
    jp2.foreachRDD(new VoidFunction<JavaPairRDD<String,String>>() {
      
      @Override
      public void call(JavaPairRDD<String, String> t) throws Exception {
        
        System.out.println(t.collect());
        System.out.println(t);
      }
    });*/
    
    /*stream.foreachRDD(s -> {
      
      System.out.println(s);
    });*/
    
    /*stream.foreachRDD(rdd -> {
    rdd.foreach(x -> {
      System.out.println(x);
    });
  });*/
    
    //jp.print();
    
    streamingContext.start();
    streamingContext.awaitTermination();
  }
}