/*
 * spark-submit --class com.kafka.spark.demo.SparkConsumer --master local[1] spark_demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 */


package com.kafka.spark.demo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class SparkConsumer {
	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
		
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("sampletopic");
		
		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		/*
		directKafkaStream.foreachRDD(rdd -> {
		    System.out.println("--- New RDD with " + rdd.partitions().size() + " partitions and " + rdd.count() + " records");
		    rdd.foreach(record -> System.out.println(record._2));
		});
		*/
		
		directKafkaStream.foreachRDD(rdd -> {
			rdd.foreach(record -> System.out.println(record._2));
		});
		
		System.out.println("LINE: " + directKafkaStream.toString());
		directKafkaStream.print();
		ssc.start();
	    ssc.awaitTermination();
	}
}
