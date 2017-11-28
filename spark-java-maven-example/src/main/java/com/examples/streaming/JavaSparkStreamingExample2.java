package com.examples.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

/**
 * Counts words in UTF8 encoded, '\n' delimited text received from the network
 * every second.
 *
 * Usage: JavaNetworkWordCount <hostname> <port> <hostname> and <port> describe
 * the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server `$
 * nc -lk 9999` and then run the example `$ bin/run-example
 * org.apache.spark.examples.streaming.JavaNetworkWordCount localhost 9999`
 */
public final class JavaSparkStreamingExample2 {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}
		
		String brokers = args[0];
		String topics = args[1];

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		
	    // Create context with a 2 seconds batch interval
	    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
	    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

		// Create direct kafka stream with brokers and topics
		JavaDStream<String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet).map(new Function<Tuple2<String,String>, String>() {
		

			/**
					 * 
					 */
					private static final long serialVersionUID = -2486629453126381419L;

			@Override
			public String call(Tuple2<String,String> x) {
				return x._2;
			}
		});

		JavaDStream<String> words = messages.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1883777258248712917L;

			@Override
			public Iterable<String> call(String x) {
				return Arrays.asList(SPACE.split(x));
			}
		});
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 6418913819715583926L;

			@Override
			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		}).reduceByKey(new Function2<Integer, Integer, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 1313500615376957416L;

			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		wordCounts.dstream().saveAsTextFiles("./output_streaming/prueba_count", "streaming");
		jssc.start();
		jssc.awaitTermination();
	}
}