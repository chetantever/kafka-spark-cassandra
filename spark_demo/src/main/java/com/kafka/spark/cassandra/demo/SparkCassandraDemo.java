/*
 * spark-submit --class com.kafka.spark.cassandra.demo.SparkCassandraDemo --master local[1] spark_demo-0.0.1-SNAPSHOT-jar-with-dependencies.jar
 */

package com.kafka.spark.cassandra.demo;

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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public class SparkCassandraDemo {
	public static void main(String[] args) throws InterruptedException {

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(1000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("sampletopic");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class, String.class,StringDecoder.class, StringDecoder.class, kafkaParams,topics);

		//use connection pool for better performance -> http://spark.apache.org/docs/latest/streaming-programming-guide.html
		
		directKafkaStream.foreachRDD(rdd -> {
			rdd.foreachPartition(partitionOfRecords -> {
				Builder builder = Cluster.builder().addContactPoint("localhost").withPort(9042);
				Cluster cluster = builder.build();
				Session session = cluster.connect();
				while (partitionOfRecords.hasNext()) {
					session.execute("INSERT INTO java_api.system_metrics JSON '" + partitionOfRecords.next().toString().replace("(null,","").replaceFirst("\"", "").replaceAll("\"$", "").replace("\\", "").trim() + "';");
				}
				session.close();
		        cluster.close();
			});
		});
		ssc.start();
		ssc.awaitTermination();
	}
}
